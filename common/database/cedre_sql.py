#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
cedre_sql.py

@Purpose: SQL queries specific to CEDRE_POLREP schema.
Extends the base CopQuery class.

@version: 1.0.2
@date 2026-02-17
"""

# Nota: Ya no necesitamos importar UUID porque pasaremos cadenas
from common.database.cop_sql import CopQuery


class CedreQuery(CopQuery):
    """
    Extension of CopQuery to handle insertions into the CEDRE_POLREP schema.
    """

    def insert_incident(self, incident_data):
        """
        Inserts an incident and returns the generated ID.
        """
        query = """
        INSERT INTO CEDRE_POLREP.incident (
            chrono, chrono_lite, year_of_creation, number_in_year, operative_type, 
            cross_coordonnateur_id, type_rejet, toyen_alerte_poll, classification_poll, 
            qui_alerte_poll, tgi, navire_connecte, zone_geo_poll, pollution_principal, 
            reference_position
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
        RETURNING id;
        """

        ident = incident_data.get("identification", {})
        chrono_lite = incident_data.get('chrono_lite', incident_data.get('chronoLite'))

        ref_pos = incident_data.get("referencePosition", {})
        lon = ref_pos.get("longitude")
        lat = ref_pos.get("latitude")

        if lon is None and "coordinates" in ref_pos:
            lon = ref_pos["coordinates"][0]
            lat = ref_pos["coordinates"][1]

        values = (
            incident_data.get("chrono"),
            chrono_lite,
            ident.get("yearOfCreation"),
            ident.get("numberInYear"),
            ident.get("operativeType"),
            incident_data.get("crossCoordonnateurId"),
            incident_data.get("typeRejet"),
            incident_data.get("toyenAlertePoll"),
            incident_data.get("classificationPoll"),
            incident_data.get("quiAlertePoll"),
            incident_data.get("tgi"),
            incident_data.get("navireConnecte"),
            incident_data.get("zoneGeoPoll"),
            incident_data.get("pollutionPrincipal"),
            lon,
            lat
        )

        cursor = self.con.cursor()
        cursor.execute(query, values)
        incident_id = cursor.fetchone()[0]
        self.con.commit()
        cursor.close()

        return incident_id

    def insert_pollution(self, pollution_data, incident_id):
        query = """
        INSERT INTO CEDRE_POLREP.pollution (id, incident_id, gdh, type_polluant, forme, is_rectangle, longueur, superficie_pollution,
                               taux_couverture, comentarios, has_viscosite, has_navire_connecte, erreur, source,
                               recueil, detection_color, probability, autorite)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        polluant = pollution_data.get("polluant", {})

        # CORRECCIÓN: Se pasa el UUID como string directo del JSON
        values = (
            pollution_data["uuid"],  # Antes era UUID(pollution_data["uuid"])
            incident_id,
            pollution_data["gdh"],
            polluant.get("typePolluant"),
            polluant.get("forme"),
            polluant.get("isRectangle"),
            polluant.get("longueur"),
            polluant.get("superficiePollution"),
            polluant.get("tauxCouverture"),
            polluant.get("commentaires"),
            polluant.get("hasViscosite"),
            polluant.get("hasNavireConnecte"),
            polluant.get("erreur"),
            pollution_data.get("source"),
            pollution_data.get("recueil"),
            pollution_data.get("detectionColor"),
            pollution_data.get("probability"),
            pollution_data.get("autorite")
        )
        self.insert(query, values)

    def insert_position(self, position_data, pollution_id):
        query = """
        INSERT INTO CEDRE_POLREP.position (id, pollution_id, gdh, location, azimut, reference, distance, observation, source, empty)
        VALUES (%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s, %s, %s, %s, %s);
        """

        loc = position_data.get("location", {})

        # CORRECCIÓN: Se pasa el UUID como string directo
        values = (
            position_data["uuid"],  # Antes era UUID(...)
            pollution_id,
            position_data["gdh"],
            loc.get("longitudeDD"),
            loc.get("latitudeDD"),
            loc.get("azimut"),
            loc.get("reference"),
            loc.get("distance"),
            loc.get("observation"),
            position_data.get("source"),
            position_data.get("empty")
        )
        self.insert(query, values)

    def insert_message(self, message_data, incident_id):
        query = """
        INSERT INTO CEDRE_POLREP.message (incident_id, numero_ordre, selected_mail_list, available_mail_list, type, locale, is_validate,
                             is_sent, date_transmission, report_type, priority_level, is_warn, is_inf, is_fac,
                             date_time_warn, date_time_inf, date_time_fac, position, outflow, position_extent,
                             carac, wind, current, sea_state, drift, forecast, observer, action_taken, photo, organisations,
                             spare_inf, acknowledge_warn, acknowledge_inf)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        values = (
            incident_id,
            message_data.get("numeroOrdre"),
            message_data.get("selectedMailList"),
            message_data.get("availableMailList"),
            message_data.get("type"),
            message_data.get("locale"),
            message_data.get("isValidate"),
            message_data.get("isSent"),
            message_data.get("dateTransmission"),
            message_data.get("reportType"),
            message_data.get("priorityLevel"),
            message_data.get("isWarn"),
            message_data.get("isInf"),
            message_data.get("isFac"),
            message_data.get("dateTimeWarn"),
            message_data.get("dateTimeInf"),
            message_data.get("dateTimeFac"),
            message_data.get("position"),
            message_data.get("outflow"),
            message_data.get("position_Extent"),
            message_data.get("carac"),
            message_data.get("wind"),
            message_data.get("current"),
            message_data.get("seaState"),
            message_data.get("drift"),
            message_data.get("forecast"),
            message_data.get("observer"),
            message_data.get("actionTaken"),
            message_data.get("photo"),
            message_data.get("organisations"),
            message_data.get("spareInf"),
            message_data.get("acknowledgeWarn"),
            message_data.get("acknowledgeInf")
        )
        self.insert(query, values)

    def insert_bulletin_meteo(self, bulletin_data, incident_id):
        query = """
        INSERT INTO CEDRE_POLREP.bulletin_meteo (id, incident_id, is_pinned, position, gdh, source, type, gdh_obs, visibilite,
                                    nebulosite, secteur_vent, force_vent, direction_vent, vitesse_vent, direction_courant,
                                    vitesse_courant, etat_mer, temp_mer, temp_air)
        VALUES (%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        pos = bulletin_data.get("position", {}).get("coordinates", [0, 0])

        # CORRECCIÓN: Se pasa el UUID como string directo
        values = (
            bulletin_data["uuid"],  # Antes era UUID(...)
            incident_id,
            bulletin_data.get("isPinned"),
            pos[0],
            pos[1],
            bulletin_data.get("gdh"),
            bulletin_data.get("source"),
            bulletin_data.get("type"),
            bulletin_data.get("gdhObs"),
            bulletin_data.get("visibilite"),
            bulletin_data.get("nebulosite"),
            bulletin_data.get("secteurVent"),
            bulletin_data.get("forceVent"),
            bulletin_data.get("directionVent"),
            bulletin_data.get("vitesseVent"),
            bulletin_data.get("directionCourant"),
            bulletin_data.get("vitesseCourant"),
            bulletin_data.get("etatMer"),
            bulletin_data.get("tempMer"),
            bulletin_data.get("tempAir")
        )
        self.insert(query, values)