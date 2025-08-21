# -*- coding: utf-8 -*-
"""
Fire History Maker 2025 FRAS

ArcPy workflow, robust to:
  - Use C:\Data\FireHistory\Fire_History_2025\Fire_History_2025_Working.gdb for all inputs/outputs.
  - Use NPWSFireHistory as NSW input.
  - Skip any process if output already exists.
  - Prevent errors related to deleting geometry fields (Shape_Area, Shape_Length, etc.).
  - Prevent errors from trying to add a field that already exists or with a different type.
  - Ensures Burn_Date for LASTLOG, NSW, and SA is in YYYYMMDD integer format.
  - Ensures NSW fire history is merged in the final output.
  - Final output only contains Source, Burn_Date, and any forced fields (geometry).

Assumes all referenced feature classes/tables exist in the geodatabase.
"""

import arcpy

def exists(fc_name):
    """Check if a feature class or table exists in the workspace."""
    return arcpy.Exists(fc_name)

def add_and_calc_field(fc, field_name, field_type, calc_expr, code_block=None):
    """
    Add a field if not present (case-insensitive), and calculate values if type matches.
    Never tries to add if field exists, regardless of case.
    Calculation only happens if the type matches or is compatible (e.g. TEXT/STRING).
    """
    field_found = None
    for f in arcpy.ListFields(fc):
        if f.name.lower() == field_name.lower():
            field_found = f
            break
    field_type_check = field_type.upper()
    arcpy_type = field_found.type.upper() if field_found else ""
    if field_found is None:
        arcpy.AddField_management(fc, field_name, field_type)
        arcpy.CalculateField_management(fc, field_name, calc_expr, "PYTHON3", code_block if code_block else "")
    elif arcpy_type == field_type_check or (field_type_check == "TEXT" and arcpy_type == "STRING"):
        arcpy.CalculateField_management(fc, field_found.name, calc_expr, "PYTHON3", code_block if code_block else "")
    else:
        print(f"Field {field_name} exists as type {field_found.type} (not {field_type}). Skipping calculation for this field.")

def FireHistoryMakerBREPS_2025():
    gdb = r"C:\Data\FireHistory\Fire_History_2025\Fire_History_2025_Working.gdb"
    arcpy.env.workspace = gdb
    arcpy.env.overwriteOutput = True

    # Input feature classes in the GDB
    NPWSFireHistory = "NPWSFireHistory"
    VicShape_vg94 = "VicShape_vg94"
    DEECA_FIRE_HISTORY_TREATED = "FIRE_HISTORY_TREATED"
    ECOFIRE_NotfeasibletotreatLow = "ECOFIRE_NotfeasibletotreatLow"
    LASTLOG25 = "LASTLOG25"
    SA_FireHistory = "FIREMGT_FireHistory_GDA94"

    # Output feature class names (in the same GDB)
    NPWS_FH23_VG94_Er = "NPWS_FH23_VG94_Er"
    FIRE_HISTORY_TREATED_Select = "FIRE_HISTORY_TREATED_Select"
    FIRE_HISTORY_vg94_post2011_mincover = "FIRE_HISTORY_vg94_mincover"
    FIRE_HISTORY_vg94_Bushfires = "FIRE_HISTORY_vg94_Bushfires"
    FIRE_HISTORY_vg94_Burns = "FIRE_HISTORY_vg94_Burns"
    FIRE_HISTORY_vg94_Burns_Treatable = "FIRE_HISTORY_vg94_Burns_Treatable"
    LASTLOG25_filter = "LASTLOG25_filter"
    LASTLOG25_dates = "LASTLOG25_dates"
    LASTLOG_vg94_DF = "LASTLOG_vg94_DF"
    FIRE_HISTORY_vg94_merge1 = "FIRE_HISTORY_vg94_merge1"
    FIRE_HISTORY_vg94_merge1_Cli = "FIRE_HISTORY_vg94_merge1_Cli"
    SA_FH_VG94_Er = "SA_FH23_VG94_Er"
    FH_merge = "FH_merge"
    fh_merge_vg94 = "FH_merge_vg94"
    BREPS_FireHistory_2025 = "BREPS_FireHistory_2025"
    BREPS_FireHistory_2025_Clean = "BREPS_FireHistory_2025_Clean"

    # 1. NPWS FireHistory - remove Vic overlaps
    if not exists(NPWS_FH23_VG94_Er):
        arcpy.Erase_analysis(NPWSFireHistory, VicShape_vg94, NPWS_FH23_VG94_Er)

    # 2. DEECA: Select burnt only
    if not exists(FIRE_HISTORY_TREATED_Select):
        arcpy.Select_analysis(DEECA_FIRE_HISTORY_TREATED, FIRE_HISTORY_TREATED_Select, "FIRE_SEVERITY <> 'UNBURNT'")

    # 3. Calculate null dates to 20230101
    add_and_calc_field(
        FIRE_HISTORY_TREATED_Select, "START_DATE_INT", "TEXT",
        "updateDate(!START_DATE_INT!)",
        "def updateDate(value):\n  if value is None:\n    return '20230101'\n  else: return value"
    )

    # 4. Month fix (>12)
    arcpy.CalculateField_management(
        FIRE_HISTORY_TREATED_Select, "START_DATE_INT",
        "round_mth(!START_DATE_INT!)", "PYTHON3",
        "def round_mth(date):\n  if isinstance(date, int): date = str(date)\n  yr = date[0:4]\n  mth = date[4:6]\n  day = date[6:8]\n  if int(mth) > 12:\n    date = yr + '12' + day\n  return int(date)"
    )

    # 5. Add Burn_Date field (LONG) & populate from START_DATE_INT
    add_and_calc_field(FIRE_HISTORY_TREATED_Select, "Burn_Date", "LONG", "!START_DATE_INT!")

    # 6. Select min cover >=2012
    if not exists(FIRE_HISTORY_vg94_post2011_mincover):
        arcpy.Select_analysis(
            FIRE_HISTORY_TREATED_Select, FIRE_HISTORY_vg94_post2011_mincover,
            "(SEASON < 2012) OR (SEASON >= 2012 AND FIRE_COVER IN('30-49','50-69','70-89','90-100','UNKNOWN', NULL))"
        )

    # 7. Split bushfires and burns
    if not exists(FIRE_HISTORY_vg94_Bushfires):
        arcpy.Select_analysis(FIRE_HISTORY_vg94_post2011_mincover, FIRE_HISTORY_vg94_Bushfires, "FIRETYPE <> 'BURN'")
    if not exists(FIRE_HISTORY_vg94_Burns):
        arcpy.Select_analysis(
            FIRE_HISTORY_vg94_post2011_mincover, FIRE_HISTORY_vg94_Burns,
            "FIRETYPE = 'BURN' AND TREATMENT_TYPE IN('FUEL REDUCTION','ECOLOGICAL','NOT DETERMINED','OTHER')"
        )

    # 8. Erase non-treatable burns
    if not exists(FIRE_HISTORY_vg94_Burns_Treatable):
        arcpy.Erase_analysis(FIRE_HISTORY_vg94_Burns, ECOFIRE_NotfeasibletotreatLow, FIRE_HISTORY_vg94_Burns_Treatable)

    # 9. Remove unwanted fields and set Source for bushfires
    bushfire_drop_fields = [
        "FIRETYPE", "SEASON", "FIRE_NO", "NAME", "START_DATE", "START_DATE_INT", "TREATMENT_TYPE", "FIRE_SEVERITY", "FIRE_COVER",
        "FIREKEY", "CREATE_DATE", "UPDATE_DATE", "AREA_HA", "METHOD", "METHOD_COMMENTS", "ACCURACY", "DSE_ID", "CFA_ID", "DISTRICT_ID",
        "Area_calc", "Centroid_x", "Centroid_y", "Shape_length_1", "Shape_area_1", "Shape_length_12", "Shape_area_12",
        "Shape_length_12_13", "Shape_area_12_13"
    ]
    existing_bushfire_fields = [f.name for f in arcpy.ListFields(FIRE_HISTORY_vg94_Bushfires)]
    bushfire_drop = [f for f in bushfire_drop_fields if f in existing_bushfire_fields]
    if bushfire_drop:
        arcpy.DeleteField_management(FIRE_HISTORY_vg94_Bushfires, bushfire_drop)
    add_and_calc_field(FIRE_HISTORY_vg94_Bushfires, "Source", "TEXT", '"BUSHFIRES"')

    # 10. Remove unwanted fields and set Source for burns
    burns_drop_fields = bushfire_drop_fields + ["Shape_length_12_13_14", "Shape_area_12_13_14"]
    existing_burns_fields = [f.name for f in arcpy.ListFields(FIRE_HISTORY_vg94_Burns_Treatable)]
    burns_drop = [f for f in burns_drop_fields if f in existing_burns_fields]
    if burns_drop:
        arcpy.DeleteField_management(FIRE_HISTORY_vg94_Burns_Treatable, burns_drop)
    add_and_calc_field(FIRE_HISTORY_vg94_Burns_Treatable, "Source", "TEXT", '"Burns"')

    # 11. Logging history: filter, remove null dates, add Burn_Date (YYYYMMDD integer format)
    if not exists(LASTLOG25_filter):
        arcpy.Select_analysis(LASTLOG25, LASTLOG25_filter, "SILVSYS IN('CFE','GSE','RRH','STR')")
    if not exists(LASTLOG25_dates):
        arcpy.Select_analysis(LASTLOG25_filter, LASTLOG25_dates, "ENDDATE IS NOT NULL")
    # Calculate Burn_Date in YYYYMMDD format as integer
    add_and_calc_field(
        LASTLOG25_dates, "Burn_Date", "LONG",
        "format_enddate(!ENDDATE!)",
        """def format_enddate(val):
            if val is None:
                return None
            try:
                # If val is a datetime object, use strftime
                return int(val.strftime("%Y%m%d"))
            except Exception:
                try:
                    import datetime
                    dt = datetime.datetime.strptime(str(val), "%Y-%m-%d")
                    return int(dt.strftime("%Y%m%d"))
                except Exception:
                    # Fallback: try to strip separators
                    return int(str(val).replace('-', '').replace('/', ''))
        """
    )
    lastlog_drop_fields = [
        "LOGHISTID", "FMA", "COUPEADD", "COMPART", "COUPENO", "BLOCK", "DECADE", "SEASON", "SILVSYS", "FORESTYPE",
        "STARTDATE", "MAPLOGSRC", "LH_ID", "COUPE_NAME", "ENDDATE", "HARV_ORG", "HECTARES", "X_FMA", "AREASQM",
        "X_SILVSYS", "X_BLOCK", "X_FORETYPE", "SECTION_SD"
    ]
    existing_lastlog_fields = [f.name for f in arcpy.ListFields(LASTLOG25_dates)]
    lastlog_drop = [f for f in lastlog_drop_fields if f in existing_lastlog_fields]
    if lastlog_drop:
        arcpy.DeleteField_management(LASTLOG25_dates, lastlog_drop)
    add_and_calc_field(LASTLOG25_dates, "Source", "TEXT", '"LASTLOG25"')

    # 12. Project all logging history to VicGrid94
    vicgrid = arcpy.SpatialReference(3111)  # GDA94 / VicGrid94
    if not exists(LASTLOG_vg94_DF):
        arcpy.Project_management(LASTLOG25_dates, LASTLOG_vg94_DF, vicgrid)

    # 13. Merge bushfires, burns, logging history
    if not exists(FIRE_HISTORY_vg94_merge1):
        arcpy.Merge_management(
            [FIRE_HISTORY_vg94_Bushfires, FIRE_HISTORY_vg94_Burns_Treatable, LASTLOG_vg94_DF],
            FIRE_HISTORY_vg94_merge1
        )

    # 14. Clip to VicShape
    if not exists(FIRE_HISTORY_vg94_merge1_Cli):
        arcpy.Clip_analysis(FIRE_HISTORY_vg94_merge1, VicShape_vg94, FIRE_HISTORY_vg94_merge1_Cli)

    # 15. SA fire history erase, add fields, delete non-geometry fields
    if not exists(SA_FH_VG94_Er):
        arcpy.Erase_analysis(SA_FireHistory, VicShape_vg94, SA_FH_VG94_Er)
    add_and_calc_field(SA_FH_VG94_Er, "Burn_Date", "LONG",
        "format_firedate(!FIREDATE!)",
        """def format_firedate(val):
            if val is None:
                return None
            try:
                return int(val.strftime("%Y%m%d"))
            except Exception:
                try:
                    import datetime
                    dt = datetime.datetime.strptime(str(val), "%Y-%m-%d")
                    return int(dt.strftime("%Y%m%d"))
                except Exception:
                    return int(str(val).replace('-', '').replace('/', ''))
        """
    )
    add_and_calc_field(SA_FH_VG94_Er, "Source", "TEXT", '"SA"')
    sa_drop_fields = [
        "CAPTUREMET", "CAPTURESOU", "COMMENTS", "DATERELIAB", "FEATURESOU", "FINANCIALY", "FIREDATE", "FIREYEAR",
        "HECTARES", "IMAGEINFOR", "INCIDENTNA", "INCIDENTNU", "INCIDENTTY", "SEASON"
        # Do NOT include Shape_Area or Shape_Length!
    ]
    existing_sa_fields = [f.name for f in arcpy.ListFields(SA_FH_VG94_Er)]
    sa_drop = [f for f in sa_drop_fields if f in existing_sa_fields]
    if sa_drop:
        arcpy.DeleteField_management(SA_FH_VG94_Er, sa_drop)

    # 15b. NSW fire history: ensure Burn_Date and Source are set before merging
    add_and_calc_field(NPWS_FH23_VG94_Er, "Burn_Date", "LONG",
        "format_startdate(!StartDate!)",
        """def format_startdate(val):
            if val is None:
                return None
            try:
                return int(val.strftime("%Y%m%d"))
            except Exception:
                try:
                    import datetime
                    dt = datetime.datetime.strptime(str(val), "%Y-%m-%d")
                    return int(dt.strftime("%Y%m%d"))
                except Exception:
                    return int(str(val).replace('-', '').replace('/', ''))
        """
    )
    add_and_calc_field(NPWS_FH23_VG94_Er, "Source", "TEXT", '"NSW"')

    # 16. Merge all states together - including NSW (NPWS_FH23_VG94_Er)
    if not exists(FH_merge):
        arcpy.Merge_management(
            [NPWS_FH23_VG94_Er, FIRE_HISTORY_vg94_merge1_Cli, SA_FH_VG94_Er],
            FH_merge
        )

    # 17. Project to VicGrid94
    if not exists(fh_merge_vg94):
        arcpy.Project_management(FH_merge, fh_merge_vg94, vicgrid)

    # 18. Final cleanup: keep only Source, Burn_Date, and geometry fields
    keep_fields = ["Source", "Burn_Date"]
    geometry_fields = []
    required_fields = ["OBJECTID", "Shape", "Shape_Area", "Shape_Length"]

    for f in arcpy.ListFields(fh_merge_vg94):
        if f.type.lower() in ["geometry", "shape"]:
            geometry_fields.append(f.name)
    keep_fields += geometry_fields

    # Delete all other fields EXCEPT required ones
    all_fields = [f.name for f in arcpy.ListFields(fh_merge_vg94)]
    drop_fields = [
        f for f in all_fields
        if f not in keep_fields and f.upper() not in [r.upper() for r in required_fields]
    ]
    if drop_fields:
        arcpy.DeleteField_management(fh_merge_vg94, drop_fields)

    # 19. Sort by Burn_Date, output as final
    if not exists(BREPS_FireHistory_2025):
        arcpy.Sort_management(fh_merge_vg94, BREPS_FireHistory_2025, [["Burn_Date", "DESCENDING"]], spatial_sort_method="UR")

    # 20. Output clean version with only Source, Burn_Date, geometry
    if not exists(BREPS_FireHistory_2025_Clean):
        arcpy.CopyFeatures_management(BREPS_FireHistory_2025, BREPS_FireHistory_2025_Clean)

if __name__ == '__main__':
    FireHistoryMakerBREPS_2025()