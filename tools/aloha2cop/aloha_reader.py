#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
**aloha_reader.py**

@Purpose: Class to read aloha kml

@version: 0.0

@python version: 3.9
@author: Pedro Montero
@license: INTECMAR
@requires: fastkml

@date 2022/09/30

@history:


"""

from fastkml import kml
import xml.etree.ElementTree as ET
from shapely.geometry import Polygon


class ThreatZone:
    """Class for threat zone"""
    def __init__(self):
        self.name = None
        self.description = None
        self.coordinates = None
        self.level = None

    def set_level(self):
        words = {'WindConfidence LEL 10%': ['10% LEL', 'Wind Direction'],
                 '10% LEL': ['10% LEL', 'Threat Zone'],
                 '60% LEL': ['60% LEL', 'Threat Zone'],
                 'AEGL-1': ['AEGL-1', 'Threat Zone'],
                 'AEGL-2': ['AEGL-2', 'Threat Zone'],
                 'AEGL-3': ['AEGL-3', 'Threat Zone'],
                 'AEGL-1 Confidence': ['AEGL-1', 'Confidence Lines'],
                 'AEGL-2 Confidence': ['AEGL-2', 'Confidence Lines'],
                 'AEGL-3 Confidence': ['AEGL-3', 'Confidence Lines'],
                 'WindConfidence PAC-1': ['PAC-1', 'Wind Direction'],
                 'PAC-1': ['PAC-1', 'Threat Zone'],
                 'PAC-2': ['PAC-2', 'Threat Zone'],
                 'PAC-3': ['PAC-3', 'Threat Zone'],
                 'WindConfidence IDLH': ['IDLH', 'Wind Direction'],
                 'IDLH': ['IDLH', 'Threat Zone'],

                 }
        for keyword in words:
            keyword_list = words[keyword]
            if all(list(word in self.name for word in keyword_list)):
                self.level = keyword


class Aloha:
    """Class for building aloha information"""

    def __init__(self, kml_file):
        self.kml_file = kml_file
        self.threat_zones = []
        self.loc_type = None
        self.category = None
        self.parse_kml_direct()

    def parse_kml_direct(self):
        """Parse KML directly using ElementTree"""
        tree = ET.parse(self.kml_file)
        root = tree.getroot()

        # Define namespaces
        ns = {
            'kml': 'http://www.opengis.net/kml/2.2',
            'gx': 'http://www.google.com/kml/ext/2.2'
        }

        # Busca el Folder "Aloha Threat Zones"
        for folder in root.findall('.//kml:Folder', ns):
            folder_name_elem = folder.find('kml:name', ns)
            if folder_name_elem is not None and folder_name_elem.text == 'Aloha Threat Zones':
                print(f'Found Aloha Threat Zones folder')

                # Procesa cada Placemark
                for placemark in folder.findall('kml:Placemark', ns):
                    threat_zone = ThreatZone()

                    # Obtén el nombre
                    name_elem = placemark.find('kml:name', ns)
                    if name_elem is not None:
                        threat_zone.name = name_elem.text

                    # Obtén la descripción
                    desc_elem = placemark.find('kml:description', ns)
                    if desc_elem is not None:
                        threat_zone.description = desc_elem.text

                    # Obtén las coordenadas
                    coords_elem = placemark.find('.//kml:coordinates', ns)
                    if coords_elem is not None:
                        coords_text = coords_elem.text.strip()
                        # Parsea las coordenadas
                        coord_pairs = []
                        for coord in coords_text.split():
                            parts = coord.split(',')
                            if len(parts) >= 2:
                                lon, lat = float(parts[0]), float(parts[1])
                                coord_pairs.append((lon, lat))

                        # Crea el polígono con Shapely
                        if coord_pairs:
                            threat_zone.geometry = Polygon(coord_pairs)

                    # Establece el nivel
                    threat_zone.set_level()

                    if threat_zone.name:  # Solo añade si tiene nombre
                        self.threat_zones.append(threat_zone)
                        print(f'  Added: {threat_zone.name} -> {threat_zone.level}')

        print(f'\nTotal threat zones found: {len(self.threat_zones)}')

    def get_features(self):
        """Método legacy - ya no necesario"""
        return []

    def parse_threat_zones(self, features):
        """Método legacy - ya no necesario"""
        pass

    def set_loc_type(self):
        if not self.threat_zones:
            raise ValueError("No threat zones found in KML file. Check if the file format is correct.")

        loc_types = ['PAC', 'LEL', 'AEGL', 'IDLH']
        name = self.threat_zones[0].name
        print(f'Checking LOC type for: {name}')
        res = [ele in name for ele in loc_types]
        index = [i for i, val in enumerate(res) if val]
        if not index:
            raise ValueError(f"No valid LOC type found in threat zone name: {name}")
        self.loc_type = loc_types[index[0]]
        print(f'LOC type set to: {self.loc_type}')

    def set_category(self):
        category = {'PAC': 'TOXIC', 'AEGL': 'TOXIC', 'LEL': 'FLAMMABLE', 'IDLH': 'TOXIC'}
        self.category = category[self.loc_type]
        print(f'Category set to: {self.category}')


def main(file):

    aloha = Aloha(file)
    print(f'Reading {aloha.kml_file}')
    aloha.parse_threat_zones(aloha.get_features())
    for threat_zone in aloha.threat_zones:
        print(f'NAME: The name of the threat zone is {threat_zone.name}')
        print(f'     DESCRIPTION: \n     {threat_zone.description}')
        print(f'     LEVEL:     {threat_zone.level}\n\n')

    aloha.set_loc_type()
    aloha.set_category()
    print(aloha.level_type, aloha.category)





if __name__ == "__main__":
    # main('../datos/aloha/E20171123VILAGARCIA_LEL.kml')

    #main('../datos/aloha/1tmAmoniaco.kml')
    main('../datos/aloha/E20171123VILAGARCIA_PAC.kml')






