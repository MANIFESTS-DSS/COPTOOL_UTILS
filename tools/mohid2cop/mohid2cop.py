#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
MOHID2COP: Automated Data Ingestion Tool for MOHID Lagrangian Air Pollution.

Technical Description:
    This program acts as an ETL bridge between MOHID Lagrangian model outputs
    (HDF5/NetCDF) and the COP (Common Operational Picture) database. It extracts
    maximum concentration fields, computes isolines based on specified threat
    levels (AEGL, PAC, etc.), and ingests the resulting geometries into the
    database for real-time visualization.

Project: MANIFESTS-Genius
Full Title: From Gases and Evaporators risk assessmeNt towards an Integrated
            management of sea and land pollUtion incidentS.

-------------------------------------------------------------------------------
ACKNOWLEDGMENT & DISCLAIMER
-------------------------------------------------------------------------------
The work described in this report was supported by the Directorate-General for
European Civil Protection and Humanitarian Aid Operations (DG-ECHO) of the
European Union through the Grant Agreement number 101140390 - MANIFESTS
Genius – UCPM-2023-KAPP corresponding to the Call objective “Knowledge for
Action in Prevention and Preparedness”.

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

License:
    Distributed under the European Union Public Licence (EUPL) v1.2.

Author: Pedro Montero / INTECMAR
Version: 1.0.0
Date: 2026-02-16
"""

from datetime import datetime
from common.readers.inout import read_input
from mohid_reader import Mohid
from common.database.cop_sql import CopQuery


def main(inputs):
    """
    Main execution flow for MOHID data ingestion.
    """
    # 1. Extract inputs from JSON configuration
    file_in = inputs['file_in']
    campaign = inputs['campaign']
    model = inputs['model']
    simulation = inputs['simulation']
    levels = inputs['levels']

    print(f'Processing file: {file_in}')

    # 2. Initialize MOHID Reader and parse threat zones (Isolines)
    # This processes the HDF5/NetCDF to find the spatial polygons
    mohid = Mohid(file_in, levels['type'], levels['level'])
    mohid.parse_threat_zones()

    # Get the reference date from the model output
    initial_datetime = mohid.threat_zones[0].date

    # 3. Database Interaction via CopQuery
    db_query = CopQuery()
    db_query.connect()

    # Set Campaign and Model identifiers
    db_query.set_id_campaign(campaign['name'], campaign['description'])
    db_query.set_id_model(model)

    # Set Simulation context
    db_query.set_id_simulation(campaign['name'], model, simulation['name'], simulation['description'])
    id_simulation = db_query.get_id_simulation_by_chars(campaign['name'], model, simulation['name'])

    # Define the output group (LOC AREAS)
    db_query.set_id_output(id_simulation, initial_datetime, 'LOC AREAS')
    id_output = db_query.get_id_output(id_simulation, initial_datetime, 'LOC AREAS')

    # Associate the specific Level of Concern type (e.g., AEGL)
    db_query.set_id_loc(id_output, mohid.loc_type)
    id_loc = db_query.get_loc_id_by_type(id_output, mohid.loc_type)

    # 4. Ingest calculated geometries into the database
    for threat_zone in mohid.threat_zones:
        if threat_zone.coordinates:
            print(f'Ingesting Level: {threat_zone.name}')
            db_query.set_line(id_loc, threat_zone.name, threat_zone.coordinates, threat_zone.description)

    db_query.con.close()
    print('\n--------------- SUCCESSFUL INGESTION --------------')


if __name__ == "__main__":
    # Load configuration keys from JSON
    input_keys = ['file_in', 'campaign', 'model', 'simulation', 'levels']
    inputs = read_input('mohid2cop.json', input_keys)
    main(inputs)