#!/usr/bin/env python3
import os
import argparse
from datetime import date
import yaml
import pymysql
# from typing import List, Dict, Optional
import openstack
from nectarallocationclient import client as allocationclient
from nectarallocationclient.v1.allocations import Allocation
import novaclient.client
from prettytable import PrettyTable
import re

GPU_mapping = {
  'label_10de_1023': 'K40m',
  'label_10de_1b38': 'P40',
  'label_10de_1e02': 'TRTX',
  'label_10de_1021': 'K20Xm',
  'label_10de_1024': 'K40c',
  'label_10de_1b80': '1080',
  'label_10de_1eb8': 'T4',
  'label_10de_1db6': 'V100-32G'
}


def fetch_pci_device_from_db(conn):
  """
  Fetch PCI devices' information from cellv2 database

  :param conn: Database connection
  :return:
  """
  statement = '''
    select cn.host, pd.label, pd.status, instance_uuid, i.display_name, i.project_id, pd.dev_id, i.launched_at, i.terminated_at
    from pci_devices pd
    left join instances i on pd.instance_uuid = i.uuid
    left join compute_nodes cn on pd.compute_node_id = cn.id
    where pd.deleted=0 and cn.deleted=0;
  '''

  cursor = conn.cursor()
  cursor.execute(statement)
  result = cursor.fetchall()
  cursor.close()

  # replace label with GPU string name
  for r in result:
    if r['label'] in GPU_mapping:
      r['label'] = GPU_mapping[r['label']]

  return result


def fetch_project_info(project_id, client):
  """
  Get project allocation information with nectar allocation API

  :param project_id: the project ID
  :param client: Nectar allocation API client
  :return:
  """
  list_arg = {
    'project_id': project_id,
    'parent_request__isnull': True
  }

  alloc = client.allocations.list(**list_arg)
    
  if len(alloc) == 1:
    list_arg['parent_request__isnull'] = False
    parent_alloc = client.allocations.list(**list_arg)
    if len(parent_alloc) >= 1:
      for pa in parent_alloc:
        if alloc[0].start_date > pa.start_date:
          alloc[0].start_date = pa.start_date
    return alloc[0]
  else:
    return None

def find_ip(conn, server_id):
  """
  Search for a 130.216 address in the addresses dict

  :param addresses:
  :return ip:
  """

  if server_id is None:
    return ' '
  try:
    novac = novaclient.client.Client(2, session=conn.session)
    server = novac.servers.get(server_id)
    #print server.__dict__
    #print server.created
    if server.accessIPv4 is not None and  re.match(r'^130\.216\..*', server.accessIPv4) is not None:
      return server.accessIPv4
    for n in server.networks:
        for i in server.networks[n]:
          if re.match(r'^130\.216\..*', i) is not None:
            return i
  except novaclient.exceptions.NotFound:
    print( "No entry for ", server_id)
    return ''
  return ''

def list_gpus(osc_conn, db_conn):
    """
    list all GPUs and their utilisation

    :param osc_conn:
    :param db_conn:
    :return:
    """
    allocation_client = allocationclient.Client(1, session=osc_conn.session)

    devices = fetch_pci_device_from_db(db_conn)
    all_project_ids = [d['project_id'] for d in devices if d['project_id'] is not None]

    # process assigned projects but not VM running
    # assumption, 1 project can only have 1 GPU instance
    akl_gpu_flavor = osc_conn.search_flavors('akl.gpu*', get_extra=False)
    for f in akl_gpu_flavor:
        all_access = osc_conn.list_flavor_access(f.id)
        for a in all_access:
            # test if it already has an instance
            if a.project_id not in all_project_ids:
                # check if the project is still active
                project = fetch_project_info(a.project_id, allocation_client)
                if project is None: # or project.end_date < str(date.today()):
                    continue
                # we need to fix the project
                #for project in fetch_project_info:

                # get extra properties and project access info
                detail = osc_conn.get_flavor_by_id(f.id, get_extra=True)
                gpu_model = detail.extra_specs['pci_passthrough:alias'].split(':')[0]
                # print(f"{a.project_id} no instance, but has flavor {f.name}, GPU={gpu_model}")
                # find an available model and change the status
                for d in devices:
                    if d['label'] == gpu_model and d['status'] == 'available':
                        d['status'] = 'reserved'
                        d['project_id'] = a.project_id
                        break

    # update project_end date and contact
    for d in devices:
        # get project allocation info
        alloc = None
        if d['project_id'] is not None and len(d['project_id']) > 0:
            alloc = fetch_project_info(d['project_id'], allocation_client)
        #if alloc is not None:
        #    print alloc.__dict__
        if d['display_name'] is None and alloc is not None:
            d['display_name'] = alloc.project_name
        d['project_name'] = 'Auckland-CeR' if alloc is None else alloc.project_name
        d['start_date'] = 'NA' if alloc is None else alloc.start_date
        d['end_date'] = 'NA' if alloc is None else alloc.end_date
        d['contact'] = 'NA' if alloc is None else alloc.contact_email
        d['ip'] = find_ip(osc_conn, d['instance_uuid'])
        d['host'] = d['host'].replace("ntr-", "")
        d['host'] = d['host'].replace("akld2", "")

    # output
    x = PrettyTable()
    x.field_names = ['host', 'label', 'status', 'instance_uuid', 'display_name', 'project_name', 'project_id', 'start_date', 'end_date', 'contact', 'dev_id', 'ip', 'launched_at', 'terminated_at']
    for d in devices:
        row_data = []
        for f in x.field_names:
            row_data.append(d[f])
        x.add_row(row_data)
    print(x)


def list_user_projects(email, conn):
    """
    user id --> all projects (including closed ones) --> flavor

    :param email: email as user ID
    :param conn: openstack connection
    :return: 
    """
    allocation_client = allocationclient.Client(1, session=conn.session)

    # step 1: get the user ID from email
    user = conn.get_user(email)
    if user is None:
        print('can not find user')
        return

    # step 2: get all the projects of a user
    roles = conn.list_role_assignments(
        {
            'user': user.id
        }
    )
    all_projects = [r.project for r in roles]
    # print(all_projects)
    # step 3: get all auckland GPU flavors
    akl_gpu_flavors = conn.search_flavors('akl.gpu*', get_extra=False)
    user_allocations = []
    for f in akl_gpu_flavors:
        for access in conn.list_flavor_access(f.id):
            # print(access)
            if access.project_id in all_projects:
                alloc = fetch_project_info(access.project_id, allocation_client)
                if alloc:
                    user_allocations.append(alloc)
                    # print(f'{alloc.project_name}({alloc.project_id}): {alloc.start_date} -- {alloc.end_date}')
    # output
    x = PrettyTable()
    x.field_names = ['project_name', 'project_id', 'status', 'start_date', 'end_date']
    for a in user_allocations:
        x.add_row(
            [a.project_name, a.project_id, a.status_display, a.start_date, a.end_date]
        )
    print(x)

def fetch_gpu_nodes( gpudb_conn ):
  statement = 'SELECT hypervisor, pci_id FROM gpu_nodes WHERE active = 1'

  gpu_nodes = {}

  try:
    cursor = gpudb_conn.cursor()
    cursor.execute(statement)
    result = cursor.fetchall()
    for r in result:
      #For all the current active nodes in the gpu_nodes table, set to not seen yet
      gpu_nodes[r['hypervisor'] + ' ' + r['pci_id']] = { 'present': False, 'keys': r }
      return gpu_nodes
  except pymysql.Error as e:
    print( "DB Update gpu_nodes: ", e )
    return None


def update_gpu_db(osc_conn, db_conn, gpudb_conn):
  """
  save current metadata on current instances that are using a GPU

  :param osc_conn:
  :param gpudb_conn:
  :return:
  """
  allocation_client = allocationclient.Client(1, session=osc_conn.session)

  devices = fetch_pci_device_from_db(db_conn) 
  all_project_ids = [d['project_id'] for d in devices if d['project_id'] is not None]

  # process assigned projects but not VM running
  # assumption, 1 project can only have 1 GPU instance
  akl_gpu_flavor = osc_conn.search_flavors('akl.gpu*', get_extra=False)
  for f in akl_gpu_flavor:
    all_access = osc_conn.list_flavor_access(f.id)
    for a in all_access:
      # test if it already has an instance
      if a.project_id not in all_project_ids:
        # check if the project is still active
        project = fetch_project_info(a.project_id, allocation_client)
        if project is None: # or project.end_date < str(date.today()):
          continue
        # we need to fix the project
        #for project in fetch_project_info:

        # get extra properties and project access info
        detail = osc_conn.get_flavor_by_id(f.id, get_extra=True)
        gpu_model = detail.extra_specs['pci_passthrough:alias'].split(':')[0]
        # print(f"{a.project_id} no instance, but has flavor {f.name}, GPU={gpu_model}")
        # find an available model and change the status
        for d in devices:
          if d['label'] == gpu_model and d['status'] == 'available':
            d['status'] = 'reserved'
            d['project_id'] = a.project_id
            break

  current_gpu_node_list = fetch_gpu_nodes(gpudb_conn)

  # update project_end date and contact
  for d in devices:
    # get project allocation info
    alloc = None
    if d['project_id'] is not None and len(d['project_id']) > 0:
      alloc = fetch_project_info(d['project_id'], allocation_client)
    #if alloc is not None:
    #    print alloc.__dict__
    if d['display_name'] is None and alloc is not None:
      d['display_name'] = alloc.project_name
    d['project_name'] = 'Auckland-CeR' if alloc is None else alloc.project_name
    d['start_date'] = '2017-07-04' if alloc is None else alloc.start_date
    d['end_date'] = '9999-12-31' if alloc is None else alloc.end_date 
    if d['launched_at'] is None:
      d['launched_at'] = d['start_date']
    if d['terminated_at'] is None:
      d['terminated_at'] = d['end_date']
    d['contact'] = None if alloc is None else alloc.contact_email
    d['ip'] = find_ip(osc_conn, d['instance_uuid'])
    d['host'] = d['host'].replace("ntr-", "")
    d['host'] = d['host'].replace("akld2", "")

    statement1 = '''
    INSERT INTO ip2project (ip, project_name, project_uuid, start_date, end_date,  email, instance_uuid, instance_name, instance_launched_at, instance_terminated_at )
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE instance_terminated_at = %s, end_date = %s, final = 0
    '''

    statement2 = '''
    INSERT INTO gpu_nodes ( hypervisor, gpu_type, pci_id, active ) VALUES ( %s, %s, %s, %s )  ON DUPLICATE KEY UPDATE active = active
    '''

    statement3 = '''
    INSERT into ip2project_gpu_nodes (ip2project_id, gpu_node_id) values 
    ((select ip2project.id from ip2project where ip = %s and project_uuid = %s and instance_uuid = %s), (select gpu_nodes.id from gpu_nodes where hypervisor = %s and pci_id = %s)) ON DUPLICATE KEY UPDATE ip2project_id = ip2project_id
    '''

    if current_gpu_node_list is not None:
      key = d['host'] + ' ' + d['dev_id']
      if key in current_gpu_node_list:
        current_gpu_node_list[key]['present'] = True

    cursor = gpudb_conn.cursor()
    try:
      cursor.execute(statement2, (d['host'], d['label'], d['dev_id'], '1') )
      gpudb_conn.commit()
    except pymysql.Error as e:
      print( "DB Update gpu_nodes: ", e)

    if d['instance_uuid'] is not None and d['ip'] is not None and d['ip'] != '':
      cursor = gpudb_conn.cursor()
      try:
        cursor.execute(statement1, (d['ip'],d['project_name'],d['project_id'],d['start_date'],d['end_date'],d['contact'],d['instance_uuid'],d['display_name'],d['launched_at'],d['terminated_at'],d['terminated_at'],d['end_date']) )
        cursor.execute(statement3, (d['ip'], d['project_id'], d['instance_uuid'], d['host'], d['dev_id']) )
        gpudb_conn.commit()
      except pymysql.Error as e:
        print( "DB Update gpu_nodes: ", e)
        gpudb_conn.rollback()
    elif d['project_id'] is not None:
      statement4 = '''
      INSERT INTO gpu_booking (project_name, project_uuid, booking_start_date, booking_end_date,  email, gpu_type, count )
      VALUES(%s,%s,%s,%s,%s,%s,%s)
      ON DUPLICATE KEY UPDATE booking_end_date = %s
      '''
      cursor = gpudb_conn.cursor()
      try:
        cursor.execute(statement4, (d['project_name'],d['project_id'],d['start_date'],d['end_date'],d['contact'],d['label'],'1',d['end_date']) )
        gpudb_conn.commit()
      except pymysql.Error as e:
        print( "DB Update gpu_booking: ", e)
        gpudb_conn.rollback()

  if current_gpu_node_list is not None:
    for pd in current_gpu_node_list:
      if current_gpu_node_list[pd]['present'] == False:
        statement5 = 'UPDATE gpu_nodes SET active = 0 WHERE hypervisor = %s AND pci_id = %s'
        cursor = gpudb_conn.cursor()
        try:
          cursor.execute(statement5, (current_gpu_node_list[pd]['keys']['hypervisor'], current_gpu_node_list[pd]['keys']['pci_id']) )
          gpudb_conn.commit()
        except pymysql.Error as e:
          print( "DB Update gpu_nodes: ", e)


def clean_up_ip2project_instance_dates(nova_conn, gpudb_conn):
  
  gpu_fetch_statement = 'SELECT id, instance_uuid FROM ip2project WHERE final = 0'
  nova_fetch_statement = 'SELECT terminated_at FROM instances WHERE uuid = %s'
  gpu_update_statement = 'UPDATE ip2project SET instance_terminated_at = %s, final = 1 WHERE id = %s'
  
  gpu_read_cursor = gpudb_conn.cursor()  
  try:
    gpu_read_cursor.execute(gpu_fetch_statement)
    gpu_instances = gpu_read_cursor.fetchall()
  except pymysql.Error as e:
    print( "clean_up_ip2project_instance_dates fetch: ", e)
    gpu_read_cursor.close()
    return
  gpu_read_cursor.close()
  
  nova_cursor = nova_conn.cursor()
  gpu_update_cursor = gpudb_conn.cursor() 
  
  try:
    for i in gpu_instances:
      nova_cursor.execute(nova_fetch_statement, (i['instance_uuid']))
      nova_instance = nova_cursor.fetchall()
      for ni in nova_instance:
        if ni['terminated_at'] is not None:
          gpu_update_cursor.execute(gpu_update_statement, (ni['terminated_at'], i['id']))
          gpudb_conn.commit()
        #else:
        #  print "Update not necessary for ", i['id']
  except pymysql.Error as e:
    print( "clean_up_ip2project_instance_dates update: ", e)

  gpu_update_cursor.close()
  nova_cursor.close()
  

def main():
    filepath = os.path.dirname(os.path.realpath(__file__)) + '/../etc/db.yaml'
    with open(filepath) as file:
        config = yaml.safe_load(file)
    db_conf = config['database']
    nectar_conf = config['nectar']
    gpudb_conf = config['gpudb']

    db_opts = {
        'host': db_conf['host'],
        'port': db_conf['port'],
        'db': db_conf['db'],
        'user': db_conf['username'],
        'password': db_conf['password'],
        'charset': 'utf8',
        'use_unicode': True,
        'cursorclass': pymysql.cursors.DictCursor
    }
    db_conn = pymysql.connect(**db_opts)

    gpudb_opts = {
        'host': gpudb_conf['host'],
        'port': gpudb_conf['port'],
        'db': gpudb_conf['db'],
        'user': gpudb_conf['username'],
        'password': gpudb_conf['password'],
        'charset': 'utf8',
        'use_unicode': True,
        'cursorclass': pymysql.cursors.DictCursor
    }
    gpudb_conn = pymysql.connect(**gpudb_opts)
    
    '''
    if 'auth_type' in nectar_conf and nectar_conf['auth_type'] == 'v3applicationcredential':
      auth = v3.ApplicationCredential( auth_url=conf['auth_url'],
                       application_credential_id=conf['application_credential_id'],
                       application_credential_secret=conf['application_credential_secret'],
                     )
    else:
      auth = v3.Password(auth_url=conf['auth_url'],
                         username=conf['username'],
                         password=conf['password'],
                         project_name=conf['project_name'],
                         project_domain_name=conf['project_domain_name'],
                         user_domain_name=conf['user_domain_name']
                         )
    sess = session.Session(auth=auth)
                         
    osc_conn = openstack.connect(
        auth_url=nectar_conf['auth_url'],
        project_name=nectar_conf['project_name'],
        username=nectar_conf['username'],
        password=nectar_conf['password'],
        project_domain_name='Default',
        user_domain_name='Default'
    )
    '''
    osc_conn = openstack.connect()
    
    parser = argparse.ArgumentParser(description='Nectar GPU script')
    parser.add_argument('-u', '--user', help='email as the user id')

    args = parser.parse_args()

    if args.user:
        list_user_projects(args.user, osc_conn)
    else:
        # default to list GPU
        list_gpus(osc_conn, db_conn)

    #update_gpu_db(osc_conn, db_conn, gpudb_conn)
    #clean_up_ip2project_instance_dates( db_conn, gpudb_conn )

    db_conn.close()
    osc_conn.close()


if __name__ == '__main__':
    main()
