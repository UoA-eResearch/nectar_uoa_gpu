#!/usr/bin/python
import os
import argparse
from datetime import date
import yaml
import pymysql
# from typing import List, Dict, Optional
import openstack
from nectarallocationclient import client as allocationclient
from nectarallocationclient.v1.allocations import Allocation
from prettytable import PrettyTable
import novaclient.client
import re
import operator

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
        r['label'] = GPU_mapping[r['label']]
        #Abbreviate hostnames
        r['host'] = r['host'].replace("ntr-", "")
        r['host'] = r['host'].replace("akld2", "")

    return sorted(result, key = operator.itemgetter('label','host', 'dev_id'))


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

    #find the first start date (projects can get extended, so can have multiple records)
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
    return ''
  return ''

def list_gpus(args, osc_conn, db_conn):
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
                if project is None or project.end_date < str(date.today()):
                    continue
                # we need to fix the project

                # get extra properties and project access info
                detail = osc_conn.get_flavor_by_id(f.id, get_extra=True)
                gpu_model = detail.extra_specs['pci_passthrough:alias'].split(':')[0]
                # print(f"{a.project_id} no instance, but has flavor {f.name}, GPU={gpu_model}")
                # find an available model and change the status
                status = 'conflict'
                for d in devices:
                    if d['label'] == gpu_model and d['status'] == 'available':
                        status = 'reserved'
                        d['status'] = 'reserved'
                        d['project_id'] = a.project_id
                        break
                if status == 'conflict' and project.end_date > str(date.today()):
                    devices.append({'host': '', 'label':gpu_model, 'status': status, 'instance_uuid': '', 'display_name': '', 'project_id': a.project_id, 'dev_id': '', 'launched_at': '', 'terminated_at': '' })


    # update project_end date and contact
    for d in devices:
        # get project allocation info
        alloc = None
        if d['project_id'] is not None and len(d['project_id']) > 0:
            alloc = fetch_project_info(d['project_id'], allocation_client)
        else:
           d['project_id'] = ''
        if d['display_name'] is None:
            if alloc is not None:
                d['display_name'] = alloc.project_name
            else:
                d['display_name'] = ''
        #CeR project shows up as None
        if alloc is None:
            if d['status'] != 'available':
                d['project_name'] = 'Auckland-CeR'
            else: 
                d['project_name'] = '' 
        else:
            d['project_name'] = alloc.project_name
        d['start_date'] = '' if alloc is None else alloc.start_date
        d['end_date'] = '' if alloc is None else alloc.end_date
        d['contact'] = '' if alloc is None else alloc.contact_email
        d['ip'] = find_ip(osc_conn, d['instance_uuid'])
        if d['instance_uuid'] is None:
            d['instance_uuid'] = ''
        if d['launched_at'] is None:
            d['launched_at'] = ''

    # output
    x = PrettyTable()
    if args.long:
        x.field_names = ['host', 'label', 'status', 'display_name', 'project_name', 'start_date', 'end_date', 'launched_at', 'contact',  'ip', 'dev_id', 'instance_uuid', 'project_id' ]
    elif args.short:
        x.field_names = ['host', 'label', 'status', 'project_name', 'project_id', 'end_date', 'contact']
    else:
        x.field_names = ['host', 'label', 'status', 'display_name', 'project_name', 'start_date', 'end_date', 'launched_at', 'contact']
    for d in devices:
        row_data = []
        for f in x.field_names:
            row_data.append(d[f])
        x.add_row(row_data)
    print(x)


def list_user_projects(args, conn):
    """
    user id --> all projects (including closed ones) --> flavor

    :param args: long, short, email as user ID
    :param conn: openstack connection
    :return: 
    """
    allocation_client = allocationclient.Client(1, session=conn.session)

    # step 1: get the user ID from email
    user = conn.get_user(args.email)
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


def main():
    filepath = os.path.dirname(os.path.realpath(__file__)) + '/config.yaml'
    with open(filepath) as file:
        config = yaml.safe_load(file)
    db_conf = config['database']
    nectar_conf = config['nectar']

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

    osc_conn = openstack.connect(
        auth_url=nectar_conf['auth_url'],
        project_name=nectar_conf['project_name'],
        username=nectar_conf['username'],
        password=nectar_conf['password'],
        project_domain_name='Default',
        user_domain_name='Default'
    )

    parser = argparse.ArgumentParser(description='Nectar GPU script')
    parser.add_argument('-u', '--user', dest='email', help='email as the user id')
    parser.add_argument('-s', '--short', dest='short', action='store_true', help='long format')
    parser.add_argument('-l', '--long', dest='long', action='store_true', help='long format')

    args = parser.parse_args()

    if args.email:
        list_user_projects(args, osc_conn)
    else:
        # default to list GPU
        list_gpus(args, osc_conn, db_conn)

    db_conn.close()
    osc_conn.close()


if __name__ == '__main__':
    main()
