import arcpy

def exists(fc_name):
    return arcpy.Exists(fc_name)

def add_and_calc_field(fc, field_name, field_type, calc_expr, code_block=None):
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

def FireHistoryMakerFRAS_2025():
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
    FRAS_FireHistory_2025 = "FRAS_FireHistory_2025"

    # Use in-memory workspace for intermediates
    nsw_fc = "in_memory\\nsw_erased"
    deca_fc = "in_memory\\deeca_burnt"
    mincover_fc = "in_memory\\deeca_mincover"
    bushfires_fc = "in_memory\\deeca_bushfires"
    burns_fc = "in_memory\\deeca_burns"
    burns_treatable_fc = "in_memory\\deeca_burns_treatable"
    lastlog_filter_fc = "in_memory\\lastlog_filter"
    lastlog_dates_fc = "in_memory\\lastlog_dates"
    lastlog_vg94_fc = "in_memory\\lastlog_vg94"
    sa_erased_fc = "in_memory\\sa_erased"
    merged_fc = "in_memory\\merged_firehistory"
    merged_vg94_fc = "in_memory\\merged_firehistory_vg94"

    # 1. NPWS FireHistory - remove Vic overlaps
    arcpy.Erase_analysis(NPWSFireHistory, VicShape_vg94, nsw_fc)

    # 2. DEECA: Select burnt only
    arcpy.Select_analysis(DEECA_FIRE_HISTORY_TREATED, deca_fc, "FIRE_SEVERITY <> 'UNBURNT'")

    # 3. Calculate null dates to 20230101
    add_and_calc_field(
        deca_fc, "START_DATE_INT", "TEXT",
        "updateDate(!START_DATE_INT!)",
        "def updateDate(value):\n  if value is None:\n    return '20230101'\n  else: return value"
    )

    # 4. Month fix (>12)
    arcpy.CalculateField_management(
        deca_fc, "START_DATE_INT",
        "round_mth(!START_DATE_INT!)", "PYTHON3",
        "def round_mth(date):\n  if isinstance(date, int): date = str(date)\n  yr = date[0:4]\n  mth = date[4:6]\n  day = date[6:8]\n  if int(mth) > 12:\n    date = yr + '12' + day\n  return int(date)"
    )

    # 5. Add Burn_Date field (LONG) & populate from START_DATE_INT
    add_and_calc_field(deca_fc, "Burn_Date", "LONG", "!START_DATE_INT!")

    # 6. Select min cover >=2012 -- bushfires always included regardless of fire_cover
    arcpy.Select_analysis(
        deca_fc, mincover_fc,
        "(SEASON < 2012) OR (FIRETYPE <> 'BURN') OR (SEASON >= 2012 AND FIRETYPE = 'BURN' AND (FIRE_COVER IN('30-49','50-69','70-89','90-100','UNKNOWN') OR FIRE_COVER IS NULL))"
    )

    # 7. Split bushfires and burns -- includes null/blank TREATMENT_TYPE for burns
    arcpy.Select_analysis(mincover_fc, bushfires_fc, "FIRETYPE <> 'BURN'")
    arcpy.Select_analysis(
        mincover_fc, burns_fc,
        "FIRETYPE = 'BURN' AND (TREATMENT_TYPE IN('FUEL REDUCTION','ECOLOGICAL','NOT DETERMINED','OTHER') OR TREATMENT_TYPE IS NULL)"
    )

    # 8. Erase non-treatable burns
    arcpy.Erase_analysis(burns_fc, ECOFIRE_NotfeasibletotreatLow, burns_treatable_fc)

    # 9. Remove unwanted fields and set Source for bushfires
    bushfire_drop_fields = [
        "FIRETYPE", "SEASON", "FIRE_NO", "NAME", "START_DATE", "START_DATE_INT", "TREATMENT_TYPE", "FIRE_SEVERITY", "FIRE_COVER",
        "FIREKEY", "CREATE_DATE", "UPDATE_DATE", "AREA_HA", "METHOD", "METHOD_COMMENTS", "ACCURACY", "DSE_ID", "CFA_ID", "DISTRICT_ID",
        "Area_calc", "Centroid_x", "Centroid_y", "Shape_length_1", "Shape_area_1", "Shape_length_12", "Shape_area_12",
        "Shape_length_12_13", "Shape_area_12_13"
    ]
    existing_bushfire_fields = [f.name for f in arcpy.ListFields(bushfires_fc)]
    bushfire_drop = [f for f in bushfire_drop_fields if f in existing_bushfire_fields]
    if bushfire_drop:
        arcpy.DeleteField_management(bushfires_fc, bushfire_drop)
    add_and_calc_field(bushfires_fc, "Source", "TEXT", '"BUSHFIRES"')

    # 10. Remove unwanted fields and set Source for burns
    burns_drop_fields = bushfire_drop_fields + ["Shape_length_12_13_14", "Shape_area_12_13_14"]
    existing_burns_fields = [f.name for f in arcpy.ListFields(burns_treatable_fc)]
    burns_drop = [f for f in burns_drop_fields if f in existing_burns_fields]
    if burns_drop:
        arcpy.DeleteField_management(burns_treatable_fc, burns_drop)
    add_and_calc_field(burns_treatable_fc, "Source", "TEXT", '"Burns"')

    # 11. Logging history: filter, remove null dates, add Burn_Date (YYYYMMDD integer format)
    arcpy.Select_analysis(LASTLOG25, lastlog_filter_fc, "SILVSYS IN('CFE','GSE','RRH','STR')")
    arcpy.Select_analysis(lastlog_filter_fc, lastlog_dates_fc, "ENDDATE IS NOT NULL")
    add_and_calc_field(
        lastlog_dates_fc, "Burn_Date", "LONG",
        "format_enddate(!ENDDATE!)",
        """def format_enddate(val):
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
    lastlog_drop_fields = [
        "LOGHISTID", "FMA", "COUPEADD", "COMPART", "COUPENO", "BLOCK", "DECADE", "SEASON", "SILVSYS", "FORESTYPE",
        "STARTDATE", "MAPLOGSRC", "LH_ID", "COUPE_NAME", "ENDDATE", "HARV_ORG", "HECTARES", "X_FMA", "AREASQM",
        "X_SILVSYS", "X_BLOCK", "X_FORETYPE", "SECTION_SD"
    ]
    existing_lastlog_fields = [f.name for f in arcpy.ListFields(lastlog_dates_fc)]
    lastlog_drop = [f for f in lastlog_drop_fields if f in existing_lastlog_fields]
    if lastlog_drop:
        arcpy.DeleteField_management(lastlog_dates_fc, lastlog_drop)
    add_and_calc_field(lastlog_dates_fc, "Source", "TEXT", '"LASTLOG25"')
    vicgrid = arcpy.SpatialReference(3111)  # GDA94 / VicGrid94
    arcpy.Project_management(lastlog_dates_fc, lastlog_vg94_fc, vicgrid)

    # 12. SA fire history erase, add fields, delete non-geometry fields
    arcpy.Erase_analysis(SA_FireHistory, VicShape_vg94, sa_erased_fc)
    add_and_calc_field(sa_erased_fc, "Burn_Date", "LONG",
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
    add_and_calc_field(sa_erased_fc, "Source", "TEXT", '"SA"')
    sa_drop_fields = [
        "CAPTUREMET", "CAPTURESOU", "COMMENTS", "DATERELIAB", "FEATURESOU", "FINANCIALY", "FIREDATE", "FIREYEAR",
        "HECTARES", "IMAGEINFOR", "INCIDENTNA", "INCIDENTNU", "INCIDENTTY", "SEASON"
    ]
    existing_sa_fields = [f.name for f in arcpy.ListFields(sa_erased_fc)]
    sa_drop = [f for f in sa_drop_fields if f in existing_sa_fields]
    if sa_drop:
        arcpy.DeleteField_management(sa_erased_fc, sa_drop)

    # 13. NSW fire history: ensure Burn_Date and Source are set before merging (now uses EndDate)
    add_and_calc_field(nsw_fc, "Burn_Date", "LONG",
        "format_enddate(!EndDate!)",
        """def format_enddate(val):
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
    add_and_calc_field(nsw_fc, "Source", "TEXT", '"NSW"')

    # 14. Merge all states together, project to VicGrid94, clean fields, and output final
    arcpy.Merge_management(
        [nsw_fc, bushfires_fc, burns_treatable_fc, lastlog_vg94_fc, sa_erased_fc],
        merged_fc
    )
    arcpy.Project_management(merged_fc, merged_vg94_fc, vicgrid)

    # Keep only Source, Burn_Date, and geometry fields
    keep_fields = ["Source", "Burn_Date"]
    geometry_fields = []
    required_fields = ["OBJECTID", "Shape", "Shape_Area", "Shape_Length"]
    for f in arcpy.ListFields(merged_vg94_fc):
        if f.type.lower() in ["geometry", "shape"]:
            geometry_fields.append(f.name)
    keep_fields += geometry_fields
    all_fields = [f.name for f in arcpy.ListFields(merged_vg94_fc)]
    drop_fields = [
        f for f in all_fields
        if f not in keep_fields and f.upper() not in [r.upper() for r in required_fields]
    ]
    if drop_fields:
        arcpy.DeleteField_management(merged_vg94_fc, drop_fields)

    # Final output only
    arcpy.Sort_management(merged_vg94_fc, FRAS_FireHistory_2025, [["Burn_Date", "DESCENDING"]], spatial_sort_method="UR")

if __name__ == '__main__':
    FireHistoryMakerFRAS_2025()
