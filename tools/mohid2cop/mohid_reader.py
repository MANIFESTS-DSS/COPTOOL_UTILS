"""
mohid_reader: Spatial processing for MOHID outputs.
Part of the MANIFESTS-Genius Project.
"""

from skimage.measure import find_contours
from common.readers.reader_factory import read_factory
import numpy as np
from scipy.interpolate import interp1d


class IsolineExtractor:
    """
    Extracts geometric contours (isolines) from 2D datasets.
    Uses marching squares algorithm via scikit-image find_contours.
    """

    def __init__(self, latitudes, longitudes, dataset, levels):
        self.latitudes = latitudes
        self.longitudes = longitudes
        self.dataset = dataset
        self.levels = levels

    def extract_isolines(self):
        """
        Converts pixel-based contours into WKT (Well-Known Text) Polygons.
        """
        isolines = []
        for level in self.levels:
            # Detect contours at specific concentration value
            contours = find_contours(self.dataset, level)
            if contours:
                wkt = "POLYGON("
                for contour in contours:
                    # Map pixel indexes back to real Lat/Lon via interpolation
                    lats_contour = self.interpolate_array(contour[:, 1], self.latitudes)
                    lons_contour = self.interpolate_array(contour[:, 0], self.longitudes)

                    coords = [(lat, lon) for lat, lon in zip(lats_contour, lons_contour)]
                    wkt += "("
                    for coord in coords:
                        wkt += f"{coord[0]} {coord[1]},"
                    wkt = wkt[:-1] + "),"  # Remove last comma and close ring
                wkt = wkt[:-1] + ")"  # Close polygon
                isolines.append(wkt)
        return isolines

    @staticmethod
    def interpolate_array(indexes, field):
        """Map grid indices to geographical coordinates."""
        x_interp = np.array(indexes)
        y_interp = np.array(field)
        x = np.arange(y_interp.size)
        interp_func = interp1d(x, y_interp, kind='linear', bounds_error=False, fill_value=np.nan)
        return interp_func(x_interp)


class LagrangianFile:
    def __init__(self, file_in):
        factory = read_factory(file_in)
        self.reader = factory.get_reader()
        self.dates = self.get_dates()
        self.latitudes = self.reader.latitudes
        self.longitudes = self.reader.longitudes

    def get_dates(self):
        return [self.reader.get_date(n + 1) for n, date_group in enumerate(self.reader.get_dates())]

    def get_list_of_values(self, variable):
        return [self.reader.get_variable(variable, n+1)[:] for n, date in enumerate(self.dates)]

    def get_maximum_field(self, variable):
        return np.maximum.reduce(self.get_list_of_values(variable))

class ThreatZone:
    """Class for threat zone"""
    def __init__(self, level):
        self.name = level['name']
        self.description = level['description']
        self.value = level['value']
        self.date = None
        self.coordinates = None


class Mohid:
    """Class for building database information from a mohid lagrangian file"""

    def __init__(self, file, loc_type, levels):
        self.file = file
        self.threat_zones = None
        self.loc_type = loc_type
        self.category = None
        self.dataset_name = 'air_concentration_2D'
        if self.loc_type == 'LC50':
            self.dataset_name = 'dissolved_concentration_2D'
        self.set_category()
        self.set_threat_zones(levels)

    def set_threat_zones(self, levels):
        self.threat_zones = [ThreatZone(level) for level in levels]

    def set_category(self):
        category = {'PAC': 'TOXIC', 'AEGL': 'TOXIC', 'LEL': 'FLAMMABLE', 'IDLH': 'TOXIC', 'LC50': 'ECOTOXIC'}
        self.category = category[self.loc_type]

    def parse_threat_zones(self):
        lag_dataset = LagrangianFile(self.file)
        latitudes = lag_dataset.latitudes
        longitudes = lag_dataset.longitudes

        print(f'dataset_name = {self.dataset_name}')
        maximum_dataset = lag_dataset.get_maximum_field(self.dataset_name)
        all_levels_values = [threat_zone.value for threat_zone in self. threat_zones]
        extractor = IsolineExtractor(longitudes, latitudes, maximum_dataset, all_levels_values)
        isolines = extractor.extract_isolines()
        print('-------->', isolines)
        for m, isoline in enumerate(isolines):
            self.threat_zones[m].coordinates = isoline
            self.threat_zones[m].date = lag_dataset.dates[0]
            print('------------------------------->',m,  self.threat_zones[m].date)





def main():

    # Example usage
    file_in = '../datos/mohid/Lagrangian_1.hdf5'
    dataset_name = 'air_concentration_2D'
    levels = [0.71, 37.52, 64.5]


    lag_dataset = LagrangianFile(file_in)

    latitudes = lag_dataset.reader.latitudes
    longitudes = lag_dataset.reader.longitudes



    for n, date in enumerate(lag_dataset.get_dates()):
        dataset = lag_dataset.reader.get_variable(dataset_name, n+1)
        print(date)



    # Extract the isolines from the HDF5 file
        extractor = IsolineExtractor(longitudes, latitudes, dataset, levels)
        isolines = extractor.extract_isolines()
        print(isolines)



if __name__ == '__main__':
    main()

