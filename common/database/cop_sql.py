#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
**alohal_sql.py**

@Purpose: All queries to aloha sql database
comticon

@version: 0.0

@python version: 3.9
@author: Pedro Montero
@license: INTECMAR
@requires: 

@date 2022/10/13

@history:


"""
import os
import sys
import json
from collections import OrderedDict

import psycopg2
from datetime import datetime


class CopQuery:

    def __init__(self):
        self.__connection_string = None
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        db_path = os.path.join(base_dir, 'config', 'db.json')
        self.__set_connection_string(db_path)
        self.con = None

    def __set_connection_string(self, input_file):
        try:
            with open(input_file, 'r') as f:
                db_config = json.load(f, object_pairs_hook=OrderedDict)
                self.__connection_string = 'host={0} port={1} dbname={2} user={3} password={4}'.format(
                    db_config['host'],
                    db_config['port'],
                    db_config['dbname'],
                    db_config['user'],
                    db_config['password'])

        except FileNotFoundError:
            print(f'File not found: {input_file} ')
            if input('Do you want to create one (y/n)?') == 'n':
                quit()
        
    def connect(self):

        try:
            self.con = psycopg2.connect(self.__connection_string)

        except psycopg2.OperationalError:
            print('CAUTION: ERROR WHEN CONNECTING')
            sys.exit()

    def query(self, query, params):
        cursor = self.con.cursor()
        cursor.execute(query, params)
        result = []
        for tupla in cursor.fetchall():
            result.append(tupla)
        cursor.close()
        return result

    def insert(self, query, values):
        cursor = self.con.cursor()
        cursor.execute(query, values)
        self.con.commit()

    ''' Table Model'''

    def get_id_model(self, model):

        query = 'SELECT id FROM hns_models.models WHERE name = %s '
        params = (model,)
        list_tuple = self.query(query, params)
        if list_tuple:
            return list_tuple[0][0]

    def set_id_model(self, name):

        model_id = self.get_id_model(name)
        if not model_id:
            print(f'Model {name} doesn´t exist')
            query = '''INSERT INTO hns_models.models(name) VALUES (%s) '''
            values = (name,)
            self.insert(query, values)
        else:
            print(f'Model {name} exists with id = {model_id}')

    ''' Table campaign'''

    def get_id_campaign(self, name):

        query = 'SELECT id FROM hns_models.campaigns WHERE name = %s '
        params = (name,)
        list_tuple = self.query(query, params)

        if list_tuple:
            return list_tuple[0][0]

    def set_id_campaign(self, name, description=''):

        campaign_id = self.get_id_campaign(name)
        if not campaign_id:
            print('not exist')
            query = 'INSERT INTO hns_models.campaigns(name, description) VALUES (%s, %s) '
            values = (name, description)
            self.insert(query, values)
        else:
            print(f'Campaign {name} exists with id = {campaign_id}')

    ''' Table simulation'''

    def get_id_simulation_by_ids(self, id_campaign, id_model, name):
        query = 'SELECT id FROM hns_models.simulations WHERE name = %s AND id_campaign = %s AND id_model = %s'
        params = (name, id_campaign, id_model,)
        list_tuple = self.query(query, params)
        if list_tuple:
            return list_tuple[0][0]

    def get_id_simulation_by_chars(self, campaign, model, name):
        id_campaign = self.get_id_campaign(campaign)
        id_model = self.get_id_model(model)
        return self.get_id_simulation_by_ids(id_campaign, id_model, name)

    def set_id_simulation(self, campaign, model, name, description):
        simulation_id = self.get_id_simulation_by_chars(campaign, model, name)
        if not simulation_id:

            print('no exist')
            id_campaign = self.get_id_campaign(campaign)
            id_model = self.get_id_model(model)
            query = 'INSERT INTO hns_models.simulations(id_campaign, id_model, name, description)' \
                    ' VALUES (%s, %s,%s, %s)'
            values = (id_campaign, id_model, name, description)
            self.insert(query, values)
        else:
            print(f'Simulation {name} exists with id = {simulation_id}')

    '''Table outputs'''

    def get_id_output(self, id_simulation, initial_date, output_type):
        query = 'SELECT id FROM hns_models.outputs WHERE id_simulation = %s AND initial_date = %s AND output_type = %s'
        params = (id_simulation, initial_date, output_type,)
        list_tuple = self.query(query, params)
        if list_tuple:
            return list_tuple[0][0]

    def set_id_output(self, id_simulation, initial_date, output_type):
        id_output = self.get_id_output(id_simulation, initial_date, output_type)
        if not id_output:
            print('Output does not exist')

            query = 'INSERT INTO hns_models.outputs(id_simulation, initial_date, output_type)' \
                    ' VALUES (%s, %s,%s)'
            values = (id_simulation, initial_date, output_type)
            print(values)
            self.insert(query, values)
        else:
            print(f'Output exists with id = {id_output}')

    def get_uid_loc_categories(self, category):
        query = 'SELECT uid FROM hns_models.loc_categories WHERE name = %s'
        params = (category,)
        list_tuple = self.query(query, params)
        if list_tuple:
            return list_tuple[0][0]

    def get_uid_loc_type(self, loc_type):
        query = 'SELECT uid FROM hns_models.loc_types WHERE type = %s'
        params = (loc_type,)
        list_tuple = self.query(query, params)
        if list_tuple:
            return list_tuple[0][0]

    def get_loc_id(self, id_output, id_type):
        query = 'SELECT id FROM hns_models.loc WHERE id_output = %s AND id_type = %s'
        params = (id_output, id_type,)
        list_tuple = self.query(query, params)
        print('id_locs:      ', params, list_tuple)
        if list_tuple:
            return list_tuple[0][0]

    def get_loc_id_by_type(self, id_output, loc_type):
        id_type = self.get_uid_loc_type(loc_type)
        return self. get_loc_id(id_output, id_type)

    def set_id_loc(self, id_output,  loc_type):
        id_loc = self.get_loc_id_by_type(id_output, loc_type)
        if not id_loc:
            print('LOC does not exist')
            id_type = self.get_uid_loc_type(loc_type)
            query = 'INSERT INTO hns_models.loc(id_output, id_type)' \
                    ' VALUES (%s, %s)'
            values = (id_output,  id_type)
            print(values)
            self.insert(query, values)
        else:
            print(f'LOC exists with id = {id_loc}')

    def get_uid_loc_level(self, level_name):
        query = 'SELECT uid FROM hns_models.loc_levels WHERE level_name = %s'
        params = (level_name,)
        list_tuple = self.query(query, params)
        if list_tuple:
            return list_tuple[0][0]

    def get_id_line(self, id_loc, id_loc_level):
        query = 'SELECT id FROM hns_models.lines WHERE id_loc = %s AND id_loc_level = %s'
        params = (id_loc, id_loc_level,)
        list_tuple = self.query(query, params)
        if list_tuple:
            return list_tuple[0][0]

    def get_id_line_by_loc_level(self, id_loc, level_name):
        id_loc_level = self.get_uid_loc_level(level_name)

        return self.get_id_line(id_loc, id_loc_level)

    def set_line(self, id_loc, level_name, envelope, description=''):

        id_line = self.get_id_line_by_loc_level(id_loc, level_name)

        if not id_line:

            id_loc_level = self.get_uid_loc_level(level_name)
            query = 'INSERT INTO hns_models.lines(id_loc, id_loc_level, envelope, description)' \
                    ' VALUES (%s, %s, ST_GeomFromText(%s, 4326), %s)'
            values = (id_loc, id_loc_level, str(envelope), description)
            self.insert(query, values)
        else:
            print(f'LOC exists with id = {id_loc}')



def main():

    aloha = AlohaQuery()
    aloha.connect()
    aloha.set_id_model("ALOHA")
    aloha.set_id_campaign('MANIFESTS Example 1')
    aloha.set_id_simulation('MANIFESTS Example 1', 'ALOHA', 'E202112141200MBS', 'Example BlueStar for MANIFESTS')
    id_simulation = aloha.get_id_simulation_by_chars('MANIFESTS Example 1', 'ALOHA', 'E202112141200MBS')
    initial_date = datetime(2021, 12, 14, 12, 00, 00)
    aloha.set_id_output(id_simulation, initial_date, 'LOC AREAS')
    id_output = aloha.get_id_output(id_simulation, initial_date, 'LOC AREAS')
    aloha.set_id_loc(id_output, 'PAC')
    print(aloha.get_uid_loc_level('AEGL-1 Confidence'))

    loc_level_name = 'IDLH'
    geometry = 'POLYGON ((-8.260725 43.424754, -8.260877 43.424776, -8.261031 43.424798, -8.261184 43.42482, -8.261336 43.424843, -8.26152 43.424864, -8.261672 43.424887, -8.261885 43.424909, -8.262069 43.424931, -8.262221 43.425198, -8.262375 43.425398, -8.262588 43.425575, -8.262803 43.425752, -8.262985 43.425952, -8.263229 43.426107, -8.263505 43.426307, -8.26378 43.426484, -8.264024 43.426684, -8.264329 43.426906, -8.264635 43.427106, -8.265001 43.427305, -8.265368 43.427527, -8.265765 43.427749, -8.266193 43.427993, -8.266681 43.428237, -8.26714 43.428481, -8.26766 43.428769, -8.26824 43.429036, -8.268851 43.429324, -8.269523 43.429635, -8.270225 43.429924, -8.27102 43.430256, -8.271875 43.430589, -8.272821 43.430966, -8.2738 43.431343, -8.274899 43.43172, -8.27609 43.43212, -8.277373 43.432563, -8.27884 43.432985, -8.280397 43.433451, -8.282139 43.433917, -8.284033 43.434427, -8.28614 43.434937, -8.288462 43.43547, -8.291058 43.436002, -8.2939 43.436579, -8.297137 43.437133, -8.300681 43.437666, -8.304652 43.438176, -8.309173 43.438553, -8.314244 43.438687, -8.316993 43.437489, -8.31736 43.436718, -8.317693 43.435935, -8.318005 43.435148, -8.318296 43.434356, -8.318567 43.43356, -8.318816 43.432761, -8.319044 43.431959, -8.319251 43.431153, -8.319437 43.430345, -8.319437 43.416988, -8.319251 43.41618, -8.319044 43.415375, -8.318816 43.414572, -8.318567 43.413773, -8.318296 43.412977, -8.318005 43.412186, -8.317693 43.411399, -8.31736 43.410615, -8.316993 43.409844, -8.314244 43.408647, -8.309173 43.40878, -8.304652 43.409157, -8.300681 43.409667, -8.297137 43.4102, -8.2939 43.410755, -8.291058 43.411331, -8.288462 43.411863, -8.28614 43.412396, -8.284033 43.412906, -8.282139 43.413417, -8.280397 43.413882, -8.27884 43.414349, -8.277373 43.41477, -8.27609 43.415213, -8.274899 43.415613, -8.2738 43.41599, -8.272821 43.416368, -8.271875 43.416744, -8.27102 43.417077, -8.270225 43.41741, -8.269523 43.417699, -8.268851 43.418009, -8.26824 43.418298, -8.26766 43.418564, -8.26714 43.418852, -8.266681 43.419096, -8.266193 43.41934, -8.265765 43.419585, -8.265368 43.419806, -8.265001 43.420028, -8.264635 43.420228, -8.264329 43.420427, -8.264024 43.420649, -8.26378 43.420849, -8.263505 43.421026, -8.263229 43.421226, -8.262985 43.421381, -8.262803 43.421581, -8.262588 43.421758, -8.262375 43.421936, -8.262221 43.422136, -8.262069 43.422402, -8.261885 43.422424, -8.261672 43.422446, -8.26152 43.422469, -8.261336 43.422491, -8.261184 43.422513, -8.261031 43.422536, -8.260877 43.422557, -8.260725 43.42258, -8.260725 43.424754))'
    geometry = 'POLYGON ((-8.261 43.422557, -8.260867 43.422562, -8.260735 43.422574, -8.260604 43.422595, -8.260478 43.422624, -8.260354 43.422662, -8.260236 43.422706, -8.260123 43.422758, -8.260018 43.422817, -8.25992 43.422882, -8.25983 43.422953, -8.259749 43.42303, -8.259677 43.423112, -8.259616 43.423197, -8.259564 43.423288, -8.259525 43.423379, -8.259496 43.423474, -8.259479 43.42357, -8.259472 43.423667, -8.259479 43.423763, -8.259496 43.423859, -8.259525 43.423954, -8.259564 43.424046, -8.259616 43.424136, -8.259677 43.424221, -8.259749 43.424303, -8.25983 43.42438, -8.25992 43.424451, -8.260018 43.424517, -8.260123 43.424575, -8.260236 43.424628, -8.260354 43.424672, -8.260478 43.424709, -8.260604 43.424738, -8.260735 43.424759, -8.260867 43.424772, -8.261 43.424776, -8.261153 43.424776, -8.261305 43.424776, -8.261459 43.424776, -8.261611 43.424776, -8.261795 43.424776, -8.261947 43.424776, -8.262161 43.424776, -8.262344 43.424776, -8.262557 43.42502, -8.262772 43.425198, -8.263016 43.425352, -8.26326 43.425508, -8.263505 43.425663, -8.26378 43.425796, -8.264085 43.425952, -8.264391 43.426107, -8.264696 43.426262, -8.265032 43.42644, -8.265399 43.426595, -8.265796 43.42675, -8.266224 43.426928, -8.266652 43.427106, -8.26714 43.427283, -8.26766 43.427461, -8.268179 43.427661, -8.268759 43.42786, -8.2694 43.428059, -8.270072 43.428259, -8.270805 43.428481, -8.271569 43.428681, -8.272425 43.428925, -8.273341 43.429146, -8.274349 43.42939, -8.275418 43.429635, -8.27658 43.429879, -8.277862 43.430123, -8.279237 43.430389, -8.280764 43.430633, -8.282413 43.4309, -8.284246 43.431144, -8.286232 43.431409, -8.288432 43.431654, -8.290845 43.431898, -8.293533 43.432098, -8.296465 43.432297, -8.299765 43.432452, -8.3034 43.432541, -8.307432 43.432541, -8.311953 43.432364, -8.316993 43.431853, -8.319437 43.430345, -8.319437 43.423667, -8.319437 43.416988, -8.316993 43.41548, -8.311953 43.414969, -8.307432 43.414792, -8.3034 43.414792, -8.299765 43.414881, -8.296465 43.415036, -8.293533 43.415236, -8.290845 43.415436, -8.288432 43.41568, -8.286232 43.415924, -8.284246 43.416189, -8.282413 43.416434, -8.280764 43.4167, -8.279237 43.416944, -8.277862 43.417211, -8.27658 43.417455, -8.275418 43.417699, -8.274349 43.417943, -8.273341 43.418187, -8.272425 43.418408, -8.271569 43.418652, -8.270805 43.418852, -8.270072 43.419074, -8.2694 43.419274, -8.268759 43.419474, -8.268179 43.419673, -8.26766 43.419873, -8.26714 43.42005, -8.266652 43.420228, -8.266224 43.420405, -8.265796 43.420583, -8.265399 43.420738, -8.265032 43.420893, -8.264696 43.421071, -8.264391 43.421226, -8.264085 43.421381, -8.26378 43.421537, -8.263505 43.42167, -8.26326 43.421825, -8.263016 43.421981, -8.262772 43.422136, -8.262557 43.422313, -8.262344 43.422557, -8.262161 43.422557, -8.261947 43.422557, -8.261795 43.422557, -8.261611 43.422557, -8.261459 43.422557, -8.261305 43.422557, -8.261153 43.422557, -8.261 43.422557))'
    description = 'Red Threat Zone 900 ppm = IDLH'
    id_loc = 1
    #aloha.set_line(id_loc, loc_level_name, geometry, description)

    aloha.con.close()


if __name__ == '__main__':
    main()

        

