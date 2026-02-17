#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
CEDRE2COP: Automated Data Ingestion Tool for Cedre JSON Incident Reports.

Technical Description:
    This program acts as an ETL bridge between Cedre JSON reports (POLREP)
    and the COP (Common Operational Picture) database. It reads the JSON structure
    containing incidents, pollutions, positions, messages, and meteo bulletins,
    and ingests them into the CEDRE_POLREP database schema.

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

Author: Pedro Montero / INTECMAR (Adapted for Cedre)
Version: 1.0.0
Date: 2026-02-17
"""

import json
from common.readers.inout import read_input
from common.database.cedre_sql import CedreQuery


def main(inputs):
    """
    Main execution flow for Cedre JSON data ingestion.
    """
    # 1. Extract inputs from JSON configuration
    file_in = inputs['file_in']

    print(f'Processing file: {file_in}')

    # 2. Read the JSON file
    # Unlike Mohid which requires complex HDF5 parsing, we can load JSON directly
    try:
        with open(file_in, 'r') as f:
            incident_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File {file_in} not found.")
        return

    # 3. Database Interaction via CedreQuery (Extends CopQuery)
    # This replaces the specific DatabaseHandler from the original script
    db_query = CedreQuery()
    db_query.connect()

    try:
        print('--> Ingesting Incident data...')
        # Insert incident and retrieve the auto-generated ID
        incident_id = db_query.insert_incident(incident_data)
        print(f'    Incident inserted successfully. ID: {incident_id}')

        # 4. Ingest Pollutions and associated Positions
        if "pollutions" in incident_data:
            print(f'--> Processing {len(incident_data["pollutions"])} pollution records...')
            for pollution in incident_data["pollutions"]:
                db_query.insert_pollution(pollution, incident_id)

                # Insert positions associated with this pollution
                if "positions" in pollution:
                    for position in pollution["positions"]:
                        db_query.insert_position(position, pollution["uuid"])

        # 5. Ingest Messages
        if "messages" in incident_data:
            print(f'--> Processing {len(incident_data["messages"])} messages...')
            for message in incident_data["messages"]:
                db_query.insert_message(message, incident_id)

        # 6. Ingest Meteo Bulletins
        if "bulletinsMeteo" in incident_data:
            print(f'--> Processing {len(incident_data["bulletinsMeteo"])} meteo bulletins...')
            for bulletin in incident_data["bulletinsMeteo"]:
                db_query.insert_bulletin_meteo(bulletin, incident_id)

        print('\n--------------- SUCCESSFUL INGESTION --------------')

    except Exception as e:
        print(f'\nCRITICAL ERROR during ingestion: {e}')
        # In a production environment, you might want to rollback here
    finally:
        # Ensure connection is closed even if errors occur
        db_query.con.close()


if __name__ == "__main__":
    # Load configuration keys from JSON
    input_keys = ['file_in']
    inputs = read_input('cedre_json2cop.json', input_keys)
    main(inputs)