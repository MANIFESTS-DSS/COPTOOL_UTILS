#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
**alohal2cop.py**

@Purpose: Add aloha kml outputs to the GIS layer of PLANCAMGAL. This is a new version of former
comticon

@version: 0.0

@python version: 3.9
@author: Pedro Montero
@license: INTECMAR
@requires: common/readers, h5py, netCDF4, numpy

@date 2022/04/20

@history:


"""
from datetime import datetime
from common.readers.inout import read_input
from aloha_reader import Aloha
from aloha_sql import AlohaQuery


def read_aloha(file_in):
    aloha = Aloha(file_in)
    print(f'Reading {aloha.kml_file}')
    # Ya no necesitas llamar a parse_threat_zones porque se hace en __init__
    # aloha.parse_threat_zones(aloha.get_features())  # ELIMINA ESTA LÍNEA

    aloha.set_loc_type()
    aloha.set_category()
    return aloha


def main(inputs):
    file_in, campaign, model, simulation, initial_date = inputs['file_in'], inputs['campaign'],\
                                                         inputs['model'], inputs['simulation'], inputs['initial date']
    print(f'file_in = {file_in}')
    print(f'campaigns = {campaign}')
    initial_datetime = datetime.strptime(initial_date, "%Y-%m-%d %H:%M:%S")

    aloha = read_aloha(file_in)

    for threat_zone in aloha.threat_zones:
       print(f'NAME: The name of the threat zone is {threat_zone.name}')
       print(f'     DESCRIPTION: \n     {threat_zone.description}')
       print(f'     LEVEL:     {threat_zone.level}\n\n')

    aloha_query = AlohaQuery()
    aloha_query.connect()

    campaign_name = campaign['name']
    campaign_description = campaign['description']
    aloha_query.set_id_campaign(campaign_name, campaign_description)

    model = inputs['model']
    aloha_query.set_id_model(model)

    simulation_name = simulation['name']
    simulation_description = simulation['description']
    aloha_query.set_id_simulation(campaign_name, model, simulation_name, simulation_description)

    id_simulation = aloha_query.get_id_simulation_by_chars(campaign_name, model, simulation_name)
    aloha_query.set_id_output(id_simulation, initial_datetime, 'LOC AREAS')

    id_output = aloha_query.get_id_output(id_simulation, initial_datetime, 'LOC AREAS')
    aloha_query.set_id_loc(id_output, aloha.loc_type)

    id_loc = aloha_query.get_loc_id_by_type(id_output, aloha.loc_type)
    for threat_zone in aloha.threat_zones:
        print(f'id_loc = {id_loc}, threat_zone.level = {threat_zone.level}')
        aloha_query.set_line(id_loc, threat_zone.level, threat_zone.geometry, threat_zone.name)

    aloha_query.con.close()
    print('\n\n---------------END--------------')


if __name__ == "__main__":
    input_keys = ['file_in', 'campaign', 'model', 'simulations', 'initial date']
    inputs = read_input('aloha2cop.json', input_keys)
    main(inputs)
