#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ALOHA2COP: Automated Data Ingestion Tool for Hazard Modeling.

Technical Description:
    This program acts as an ETL bridge between NOAA's ALOHA (Areal Locations
    of Hazardous Atmospheres) model and the COP (Common Operational Picture)
    database. It parses output data from ALOHA's KML files and ingests them
    directly into the COP system to enhance real-time situational awareness.

-------------------------------------------------------------------------------
ACKNOWLEDGMENT
-------------------------------------------------------------------------------
The work described in this report was supported by the Directorate-General for
European Civil Protection and Humanitarian Aid Operations (DG-ECHO) of the
European Union through the Grant Agreement number 101140390 - MANIFESTS
Genius – UCPM-2023-KAPP corresponding to the Call objective “Knowledge for
Action in Prevention and Preparedness”.

-------------------------------------------------------------------------------
DISCLAIMER
-------------------------------------------------------------------------------

The content of this document represents the views of the author only and is
his/her sole responsibility; it cannot be considered to reflect the views of
the European Commission and/or the Directorate-General for European Civil
Protection and Humanitarian Aid Operations (DG-ECHO) or any other body of the
European Union. The European Commission and the DG-ECHO is not responsible
for any use that may be made of the information it contains.
-------------------------------------------------------------------------------

Partners:
    Developed in cooperation with INTECMAR and IST.
    Coordinated by Cedre.

Author: Pedro Montero / INTECMAR
Version: 1.0.0
Date: 2026-02-16
"""
from datetime import datetime
from common.readers.inout import read_input
from aloha_reader import Aloha
from aloha_sql import AlohaQuery


def read_aloha(file_in):
    aloha = Aloha(file_in)
    print(f'Reading {aloha.kml_file}')
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
