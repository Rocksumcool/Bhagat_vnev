from os.path import expanduser,  join,  abspath
from pyspark.sql.functions import lit
from pyspark.sql import SparkSession
from pyspark.sql import Row
spark = (SparkSession.builder.appName('DefaultNullHandling').enableHiveSupport().getOrCreate())

#Default null handling for FAC and storing data to InsZ_INT_Fac
InsZ_INT_Fac_DF = spark.sql("""
SELECT facpk, facname, facfname, faclname, facadd1, facadd2, faccity, facdirections, facstate, faccntry, faczip , facemail, facphn1, facphn2, facfax, facurl, facreqpersonnel, regexp_replace(facnote, '[\r\n]','') as facnote, facinternalind,
(CASE WHEN facreqappr = '-2147483648' 
THEN NULL ELSE facreqappr END) AS facreqappr,
(CASE WHEN facapprfk = '-2147483648' 
THEN NULL ELSE facapprfk END) AS facapprfk, faclstupd, facusrname, faclck, factimestamp, 
(CASE WHEN factimezonefk = '-2147483648' 
THEN NULL ELSE factimezonefk END) AS factimezonefk , facbuilding,
(CASE WHEN faccountryfk = '-2147483648' 
THEN NULL ELSE faccountryfk END) AS faccountryfk, facregioncd, facshippingadd1, facshippingadd2, facshippingcity, facshippingstate, facshippingzip 
FROM mylearning_1720.FAC""")
InsZ_INT_Fac_DF.write.mode("overwrite").format('orc').saveAsTable("myLearning_1720_tempdb.InsZ_INT_Fac")

#Default null handling for LOC and storing data to InsZ_INT_LOC
InsZ_INT_LOC_DF = spark.sql("""
SELECT locpk,locfacfk,locname,
(CASE WHEN locmaxcap = '-2147483648' 
THEN NULL ELSE locmaxcap END) AS locmaxcap,locreqappr,
(CASE WHEN locapprfk = '-2147483648' 
THEN NULL ELSE locapprfk END) AS locapprfk,locurl,locreqpersonnel,loclstupd,locusrname,loctimestamp,loclck,locstatuscd,loccardinality,
(CASE WHEN loccostperhour = '-9.99999999999999E14' 
THEN NULL ELSE loccostperhour END) AS loccostperhour,regexp_replace(locdescription, '[\r\n]','') as locdescription,
(CASE WHEN aclclocflag = '-128' 
THEN NULL ELSE aclclocflag END) AS aclclocflag 
FROM mylearning_1720.LOC""")
InsZ_INT_LOC_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_LOC")

#Default null handling for MYL_Code_Decode and storing data to InsZ_INT_MYL_Code_Decode
InsZ_INT_MYL_Code_Decode_DF = spark.sql("""
SELECT cdid,cdcdtfk,TRIM(Cdcode) AS Cdcode,regexp_replace(cddecode, '[\r\n]','') as cddecode, 
(CASE WHEN cdorder = '-2147483648' 
THEN NULL ELSE cdorder END) AS cdorder,cdlstupd,cdusrname,cdlck 
FROM myLearning_1720.MYL_Code_Decode""")
InsZ_INT_MYL_Code_Decode_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_MYL_Code_Decode")

#Default null handling for MYL_CourseSponsorshipGroup and storing data to InsZ_INT_MYL_CourseSponsorshipGroup
InsZ_INT_MYL_CourseSponsorshipGroup_DF = spark.sql("""
SELECT groupid_pk, 
(CASE WHEN parentid = '-2147483648' 
THEN NULL ELSE parentid END) AS parentid,
(CASE WHEN ownerid_fk = '-2147483648' 
THEN NULL ELSE ownerid_fk END) AS ownerid_fk, name, state, geographicunitid_fk, coursetrainingcat_fk 
FROM mylearning_1720.MYL_CourseSponsorshipGroup""")
InsZ_INT_MYL_CourseSponsorshipGroup_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_MYL_CourseSponsorshipGroup")

#Default null handling for MYL_DTE and storing data to InsZ_INT_MYL_DTE
InsZ_INT_MYL_DTE_DF = spark.sql("""
SELECT dtepk,dtename,dtestate,fiscalyear,preapprovereqtraining, 
(CASE WHEN budgettraininghours = '-2147483648' 
THEN NULL ELSE budgettraininghours END) AS budgettraininghours, 
(CASE WHEN budgettrainingdnp = '-2147483648' 
THEN NULL ELSE budgettrainingdnp END) AS budgettrainingdnp,recalcytdactualamts, 
(CASE WHEN intercontinentalflightcost = '-2147483648' 
THEN NULL ELSE intercontinentalflightcost END) AS intercontinentalflightcost, 
(CASE WHEN intracontinentalflightcost = '-2147483648' 
THEN NULL ELSE intracontinentalflightcost END) AS intracontinentalflightcost, 
(CASE WHEN intercontinentalbusflightcost = '-2147483648' 
THEN NULL ELSE intercontinentalbusflightcost END) AS intercontinentalbusflightcost,preapprovecoretraining,
(CASE WHEN preapprovedhours = '-2147483648' 
THEN NULL ELSE preapprovedhours END) AS preapprovedhours, preapproveddnp, 
(CASE WHEN groundtransportationcost = '-2147483648' 
THEN NULL ELSE groundtransportationcost END) AS groundtransportationcost,enablevtr,pcgroupcode,automanualflag,allspecificmembersflag,enableoctn,preapprovesometraining,preapprovenone,checkgeo 
FROM myLearning_1720.MYL_DTE""")
InsZ_INT_MYL_DTE_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_MYL_DTE")

#Default null handling for MYL_DTEBudgetMap and storing data to InsZ_INT_MYL_DTEBudgetMap
InsZ_INT_MYL_DTEBudgetMap_DF = spark.sql("""
SELECT dtebudgetmap_pk, dtebudgetmap_dte_fk,dtebudgetmap_careertrack_fk,budgettrainingdnp,budgettraininghours,
(CASE WHEN dtebudgetmap_careerlevelgroup_fk = '-2147483648' 
THEN NULL ELSE dtebudgetmap_careerlevelgroup_fk END) AS dtebudgetmap_careerlevelgroup_fk 
FROM myLearning_1720.MYL_DTEBudgetMap""")
InsZ_INT_MYL_DTEBudgetMap_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_MYL_DTEBudgetMap")

#Default null handling for Myl_MFPBilling and storing data to InsZ_INT_Myl_MFPBilling
InsZ_TMP_Myl_MFPBilling_Filtered_DF = spark.sql("""
SELECT MFPBilling_PK,
(CASE WHEN MFPBilling_Reg_FK = '-2147483648' 
THEN NULL ELSE MFPBilling_Reg_FK END) AS MFPBilling_Reg_FK ,
(CASE WHEN MFPBilling_Act_FK = '-2147483648' 
THEN NULL ELSE MFPBilling_Act_FK END) AS MFPBilling_Act_FK ,
(CASE WHEN MFPBilling_Inst_FK = '-2147483648' 
THEN NULL ELSE MFPBilling_Inst_FK END) AS MFPBilling_Inst_FK ,TRIM(MFPBilling_ParticipantFacultyFlag) AS MFPBilling_ParticipantFacultyFlag, MFPBilling_personnelNum, MFPBilling_itemUpdatedDate, MFPBilling_itemBilledDate, MFPBilling_DebitWBS, MFPBilling_CreditWBS, MFPBilling_costElement, TRIM(MFPBilling_ChargeTypeDescription) AS MFPBilling_ChargeTypeDescription, MFPBilling_itemCategory, TRIM(MFPBilling_ItemStatus) AS MFPBilling_ItemStatus, MFPBilling_itemAmount ,
(CASE WHEN MFPBilling_itemCoefficient = '-9.99999999999999E14' 
THEN NULL ELSE MFPBilling_itemCoefficient END) AS MFPBilling_itemCoefficient ,
(CASE WHEN MFPBilling_StandardAirfare = '-9.99999999999999E14' 
THEN NULL ELSE MFPBilling_StandardAirfare END) AS MFPBilling_StandardAirfare ,
(CASE WHEN MFPBilling_AvgAirfare = '-9.99999999999999E14' 
THEN NULL ELSE MFPBilling_AvgAirfare END) AS MFPBilling_AvgAirfare,
(CASE WHEN MFPBilling_ResidentDayCount = '-2147483648' 
THEN NULL ELSE MFPBilling_ResidentDayCount END) AS MFPBilling_ResidentDayCount , 
(CASE WHEN MFPBilling_CommuterDayCount = '-2147483648' 
THEN NULL ELSE MFPBilling_CommuterDayCount END) AS MFPBilling_CommuterDayCount ,
(CASE WHEN MFPBilling_InstructionDayCount = '-9.99999999999999E14' 
THEN NULL ELSE MFPBilling_InstructionDayCount END) AS MFPBilling_InstructionDayCount,
(CASE WHEN MFPBilling_Facility_FK = '-2147483648' 
THEN NULL ELSE MFPBilling_Facility_FK END) AS MFPBilling_Facility_FK ,MFPBilling_courseCodeHist, MFPBilling_sessionCodeHist, MFPBilling_topsFacilityCdHist,Row_number() 
OVER(partition BY MFPBilling_PK 
ORDER BY MFPBilling_itemBilledDate DESC) AS row_num 
FROM mylearning_1720.Myl_MFPBilling""")
InsZ_TMP_Myl_MFPBilling_Filtered_DF_Partitioned = InsZ_TMP_Myl_MFPBilling_Filtered_DF.repartition(InsZ_TMP_Myl_MFPBilling_Filtered_DF.row_num)
InsZ_TMP_Myl_MFPBilling_DF = InsZ_TMP_Myl_MFPBilling_Filtered_DF_Partitioned.filter(InsZ_TMP_Myl_MFPBilling_Filtered_DF.row_num==1)
InsZ_TMP_Myl_MFPBilling_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_Myl_MFPBilling")

#Default null handling for myl_tmx_regstatushistory and storing data to InsZ_INT_MYL_TMX_RegStatusHistory
InsZ_TMP_MYL_TMX_RegStatusHistory_Filtered_DF = spark.sql("""
SELECT regstatushistory_pk,regstatushistory_reg_fk, enrollmentstatus,previousenrollmentstatus,enrollmentstatusts,TRIM(enrollmentReasonCd) AS enrollmentReasonCd,regexp_replace(enrollmentreasontxt, '[\r\n]','') as enrollmentreasontxt, 
(CASE WHEN regstatushistory_emp_fk = '-2147483648' 
THEN NULL ELSE regstatushistory_emp_fk END) AS regstatushistory_emp_fk, 
(CASE WHEN docentid = '-2147483648' 
THEN NULL ELSE docentid END) AS docentid,projectname,projectrole,curchargeable,projectrolloffdate,projectcontact,projectapproval,interviewer_assineddate, 
(CASE WHEN interviewer_assignedempfk = '-2147483648' 
THEN NULL ELSE interviewer_assignedempfk END) AS interviewer_assignedempfk, interview_scheduledate, 
(CASE WHEN approverse_fk = '-2147483648' 
THEN NULL ELSE approverse_fk END) AS approverse_fk, 
(CASE WHEN vendorid = '-2147483648' 
THEN NULL ELSE vendorid END) AS vendorid, Row_number() 
over(PARTITION BY regstatushistory_pk 
ORDER BY regstatushistory_reg_fk) AS row_num 
FROM myLearning_1720.myl_tmx_regstatushistory""")
InsZ_TMP_MYL_TMX_RegStatusHistory_Filtered_DF_Partitioned = InsZ_TMP_MYL_TMX_RegStatusHistory_Filtered_DF.repartition(InsZ_TMP_MYL_TMX_RegStatusHistory_Filtered_DF.row_num)
InsZ_TMP_MYL_TMX_RegStatusHistory_DF = InsZ_TMP_MYL_TMX_RegStatusHistory_Filtered_DF_Partitioned.filter(InsZ_TMP_MYL_TMX_RegStatusHistory_Filtered_DF.row_num==1)
InsZ_TMP_MYL_TMX_RegStatusHistory_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_MYL_TMX_RegStatusHistory")

#Default null handling for Org and storing data to InsZ_INT_Org
InsZ_TMP_Org_Filtered_DF = spark.sql("""
SELECT org_pk,
(CASE WHEN org_prntorgfk = '-2147483648' 
THEN NULL ELSE org_prntorgfk END) AS org_prntorgfk,
(CASE WHEN org_hiertypefk = '-2147483648' 
THEN NULL ELSE org_hiertypefk END) AS org_hiertypefk,
(CASE WHEN org_cntctempfk = '-2147483648' 
THEN NULL ELSE org_cntctempfk END) AS org_cntctempfk,org_cntctemail,org_name,org_note,org_cd,org_add1,org_add2,org_city,org_state,org_zip,org_cntry,org_phn1,org_phn2,org_fax,org_url,
(CASE WHEN org_levelval = '-32768' 
THEN NULL ELSE org_levelval END) AS org_levelval,org_lstupd,org_usrname,org_timestamp,org_lck,org_domainind,
(CASE WHEN org_defaudfk = '-2147483648' 
THEN NULL ELSE org_defaudfk END) AS org_defaudfk,
(CASE WHEN org_taxnodefk = '-2147483648' 
THEN NULL ELSE org_taxnodefk END) AS org_taxnodefk,
(CASE WHEN org_taxstartnodefk = '-2147483648' 
THEN NULL ELSE org_taxstartnodefk END) AS org_taxstartnodefk,
(CASE WHEN org_defaudkdocfk = '-2147483648' 
THEN NULL ELSE org_defaudkdocfk END) AS org_defaudkdocfk,
(CASE WHEN org_defaudvcsfk = '-2147483648' 
THEN NULL ELSE org_defaudvcsfk END) AS org_defaudvcsfk, 
(CASE WHEN org_countemp = '-2147483648' 
THEN NULL ELSE org_countemp END) AS org_countemp,org_type,org_status,org_level_classgroupcd,
(CASE WHEN org_levelwf_org_fk = '-2147483648' 
THEN NULL ELSE org_levelwf_org_fk END) AS org_levelwf_org_fk,
(CASE WHEN org_docentid = '-2147483648' 
THEN NULL ELSE org_docentid END) AS org_docentid,org_sap_code,Row_number() 
OVER(partition BY org_pk 
ORDER BY org_lstupd DESC) AS row_num 
FROM myLearning_1720.Org""")
InsZ_TMP_Org_Filtered_DF_Partitioned = InsZ_TMP_Org_Filtered_DF.repartition(InsZ_TMP_Org_Filtered_DF.row_num)
InsZ_TMP_Org_DF = InsZ_TMP_Org_Filtered_DF_Partitioned.filter(InsZ_TMP_Org_Filtered_DF.row_num==1)
InsZ_TMP_Org_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_Org")

#Default null handling for report_docentmap_coursetype and storing data to InsZ_INT_report_docentmap_coursetype
InsZ_INT_report_docentmap_coursetype_DF = spark.sql("""
SELECT 
(CASE WHEN st_course_type = '-2147483648' 
THEN NULL ELSE st_course_type END) AS st_course_type,TRIM(reported) AS reported,
TRIM(dsc_course_type) AS dsc_course_type 
FROM mylearning_1720.report_docentmap_coursetype""")
InsZ_INT_report_docentmap_coursetype_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_report_docentmap_coursetype")

#Default null handling for InsZ_TMP_TBL_TMX_Activity_Filtered and storing data to InsZ_TMP_TBL_TMX_Activity_Filtered
InsZ_TMP_TBL_TMX_Activity_Filtered_DF = spark.sql("""SELECT activity_pk, activityname,activitysearch,
(CASE WHEN activitylabelfk = '-2147483648' 
THEN NULL ELSE activitylabelfk END) AS activitylabelfk, 
(CASE WHEN prntactfk = '-2147483648' 
THEN NULL ELSE prntactfk END) AS prntactfk, code,
(CASE WHEN ccenter = '-2147483648' 
THEN NULL ELSE ccenter END) AS ccenter, activitydesc, activitydescfmt, url,active,openforreg,
(CASE WHEN noregreqd = '-2147483648' 
THEN NULL ELSE noregreqd END) AS noregreqd,
(CASE WHEN maxattempts = '-2147483648' 
THEN NULL ELSE maxattempts END) AS maxattempts,
(CASE WHEN maxattemptsperparent = '-2147483648' 
THEN NULL ELSE maxattemptsperparent END) AS maxattemptsperparent,cancelled, hidden, private,empnotes,empnotesfmt,instrnotes,instrnotesfmt, isdeleted,
(CASE WHEN CostBase = '-9.99999999999999E14' 
THEN NULL ELSE CostBase END) AS CostBase,
(CASE WHEN CostCncl = '-9.99999999999999E14' 
THEN NULL ELSE CostCncl END) AS CostCncl,
(CASE WHEN CostLateCncl = '-9.99999999999999E14' 
THEN NULL ELSE CostLateCncl END) AS CostLateCncl,
(CASE WHEN CostNoShow = '-9.99999999999999E14' 
THEN NULL ELSE CostNoShow END) AS CostNoShow,
(CASE WHEN currencyfk = '-2147483648' 
THEN NULL ELSE currencyfk END) AS currencyfk,
(CASE WHEN paytermfk = '-2147483648' 
THEN NULL ELSE paytermfk END) AS paytermfk,
(CASE WHEN ecomreqd = '-2147483648' 
THEN NULL ELSE ecomreqd END) AS ecomreqd,startdt,enddt,regdeadlinedt,regcncldeadlinedt, 
(CASE WHEN timezonefk = '-2147483648' 
THEN NULL ELSE timezonefk END) AS timezonefk,
(CASE WHEN useforconflictcheck = '-2147483648' 
THEN NULL ELSE useforconflictcheck END) AS useforconflictcheck,
(CASE WHEN reqappr = '-2147483648' 
THEN NULL ELSE reqappr END) AS reqappr,
(CASE WHEN defapprempfk = '-2147483648' 
THEN NULL ELSE defapprempfk END) AS defapprempfk,
(CASE WHEN ownerempfk = '-2147483648' 
THEN NULL ELSE ownerempfk END) AS ownerempfk,contact,reqpersonnel,
(CASE WHEN cbtlaunchmtdfk = '-2147483648' 
THEN NULL ELSE cbtlaunchmtdfk END) AS cbtlaunchmtdfk,
(CASE WHEN canbecopied = '-2147483648' 
THEN NULL ELSE canbecopied END) AS canbecopied,
(CASE WHEN canbesubscr = '-2147483648' 
THEN NULL ELSE canbesubscr END) AS canbesubscr,
(CASE WHEN canbeful = '-2147483648' 
THEN NULL ELSE canbeful END) AS canbeful,
(CASE WHEN rollupfromful = '-2147483648' 
THEN NULL ELSE rollupfromful END) AS rollupfromful,
(CASE WHEN stickyaudienceind = '-2147483648' 
THEN NULL ELSE stickyaudienceind END) AS stickyaudienceind,
(CASE WHEN copyoriginalactivity = '-2147483648' 
THEN NULL ELSE copyoriginalactivity END) AS copyoriginalactivity,
(CASE WHEN mincapacity = '-2147483648' 
THEN NULL ELSE mincapacity END) AS mincapacity,
(CASE WHEN maxcapacity = '-2147483648' 
THEN NULL ELSE maxcapacity END) AS maxcapacity,
(CASE WHEN recordsession = '-2147483648' 
THEN NULL ELSE recordsession END) AS recordsession, requiredind,
(CASE WHEN pickrule = '-2147483648' 
THEN NULL ELSE pickrule END) AS pickrule, pickruletype, usetype, restricted,
(CASE WHEN acttocomplete = '-2147483648' 
THEN NULL ELSE acttocomplete END) AS acttocomplete,
(CASE WHEN contributerollupind = '-2147483648' 
THEN NULL ELSE contributerollupind END) AS contributerollupind,
(CASE WHEN minpctgrd = '-2147483648' 
THEN NULL ELSE minpctgrd END) AS minpctgrd,
(CASE WHEN EstDur = '-9.9999999E14' 
THEN NULL ELSE EstDur END) AS EstDur,
(CASE WHEN EstCrdHrs = '-9.99999999999999E14' 
THEN NULL ELSE EstCrdHrs END) AS EstCrdHrs,
(CASE WHEN sendactcompletentf = '-2147483648' 
THEN NULL ELSE sendactcompletentf END) AS sendactcompletentf,
(CASE WHEN sendactreminderntf = '-2147483648' 
THEN NULL ELSE sendactreminderntf END) AS sendactreminderntf,
(CASE WHEN sendactregntf = '-2147483648' 
THEN NULL ELSE sendactregntf END) AS sendactregntf,
(CASE WHEN ordinal = '-2147483648' 
THEN NULL ELSE ordinal END) AS ordinal,
(CASE WHEN levelval = '-2147483648' 
THEN NULL ELSE levelval END) AS levelval,
(CASE WHEN rootactivityfk = '-2147483648' 
THEN NULL ELSE rootactivityfk END) AS rootactivityfk,
(CASE WHEN referenceactfk = '-2147483648' 
THEN NULL ELSE referenceactfk END) AS referenceactfk,
(CASE WHEN originalpk = '-2147483648' 
THEN NULL ELSE originalpk END) AS originalpk,
(CASE WHEN deffacfk = '-2147483648' 
THEN NULL ELSE deffacfk END) AS deffacfk, hdnfrmtranscript,tostagingdt,lstupd,usrname, lck, `timestamp`,regstartdt, facultyprepstartdt, facultyprependdt, coursebillingcat,createdt,
(CASE WHEN create_empfk = '-2147483648' 
THEN NULL ELSE create_empfk END) AS create_empfk, fulfillcenterlearnermaterial, fulfillcenterlearnerpostmaterial, fulfillcenterinstructormaterial, fulfillcenterinstructorpostmaterial, shippingadd1, shippingadd2, shippingcity, shippingstate, shippingzip, shippingcntry, materialscontactname, materialscontactemail, materialscontactphn, materialscontactfax,
(CASE WHEN courseRating = '-9.99999999999999E14' 
THEN NULL ELSE courseRating END) AS courseRating,
(CASE WHEN numberratings = '-2147483648' 
THEN NULL ELSE numberratings END) AS numberratings,
(CASE WHEN tuitiongraceperiod = '-2147483648' 
THEN NULL ELSE tuitiongraceperiod END) AS tuitiongraceperiod,
(CASE WHEN housinggraceperiod = '-2147483648' 
THEN NULL ELSE housinggraceperiod END) AS housinggraceperiod,
(CASE WHEN weeksregclose = '-2147483648' 
THEN NULL ELSE weeksregclose END) AS weeksregclose,
(CASE WHEN targetattendance = '-2147483648' 
THEN NULL ELSE targetattendance END) AS targetattendance, transportationequalizedflag, courseclassification, managedregistrations, travellogistics, selfservice,
(CASE WHEN statutorytuitionrate = '-2147483648' 
THEN NULL ELSE statutorytuitionrate END) AS statutorytuitionrate, allowmultisession, coursemateriallink, statuschangedt, autoschedule,allowinterested,accessoncompletion,isgenerictopic,triggervtrform, proficiencycode,
(CASE WHEN coursesponsor_fk = '-2147483648' 
THEN NULL ELSE coursesponsor_fk END) AS coursesponsor_fk,activationdate,
(CASE WHEN curriculumpriority = '-2147483648' 
THEN NULL ELSE curriculumpriority END) AS curriculumpriority,tuitionapproved,
(CASE WHEN tuitionapproveduser_fk = '-2147483648' 
THEN NULL ELSE tuitionapproveduser_fk END) AS tuitionapproveduser_fk, tuitionapproveddt, curriculumpreventveto, curriculumdisplayintrainingsched, curriculumdispcat,readytobill,
(CASE WHEN readytobilluser_fk = '-2147483648' 
THEN NULL ELSE readytobilluser_fk END) AS readytobilluser_fk, readytobilldt,
(CASE WHEN activity_sessionwbs_fk = '-2147483648' 
THEN NULL ELSE activity_sessionwbs_fk END) AS activity_sessionwbs_fk,
(CASE WHEN commuterChargePerDay = '-9.99999999999999E14' 
THEN NULL ELSE commuterChargePerDay END) AS commuterChargePerDay, 
(CASE WHEN housingCostPerNight = '-9.99999999999999E14' 
THEN NULL ELSE housingCostPerNight END) AS housingCostPerNight, 
(CASE WHEN centracompletioncriteria = '-2147483648' 
THEN NULL ELSE centracompletioncriteria END) AS centracompletioncriteria, activitydescenrolled, activitydescenrolledfmt, 
(CASE WHEN facultycommittedcapacity = '-2147483648' 
THEN NULL ELSE facultycommittedcapacity END) AS facultycommittedcapacity,
(CASE WHEN originaloccupancydaycommitment = '-2147483648' 
THEN NULL ELSE originaloccupancydaycommitment END) AS originaloccupancydaycommitment,
(CASE WHEN originalcapacity = '-2147483648' 
THEN NULL ELSE originalcapacity END) AS originalcapacity,
(CASE WHEN accessonequivalency = '-2147483648' 
THEN NULL ELSE accessonequivalency END) AS accessonequivalency,
(CASE WHEN waivers = '-2147483648' 
THEN NULL ELSE waivers END) AS waivers,
(CASE WHEN collapsecg = '-128 ' 
THEN NULL ELSE collapsecg END) AS collapsecg, canaccessvendorcontent, coursecontentcontainsaccenture, contentapproved, areemployeesofaffiliates, arecontractorspermitted, 
(CASE WHEN StatutoryCostBase = '-9.99999999999999E14' 
THEN NULL ELSE StatutoryCostBase END) AS StatutoryCostBase,seniorexecutivelevel,
(CASE WHEN enableslfeatures = '-128 ' 
THEN NULL ELSE enableslfeatures END) AS enableslfeatures,searchtags,coursearchtype,
(CASE WHEN applyacmi = '-128 ' 
THEN NULL ELSE applyacmi END) AS applyacmi,
(CASE WHEN managedbymyc = '-128 ' 
THEN NULL ELSE managedbymyc END) AS managedbymyc,
(CASE WHEN autoenrolllearners = '-128 ' 
THEN NULL ELSE autoenrolllearners END) AS autoenrolllearners, autoenrollstartdate, 
(CASE WHEN createdbydst = '-128 ' 
THEN NULL ELSE createdbydst END) AS createdbydst,billingprocess,billingevent,licensetype, recoverywbse,allowlearnertocancel,
(CASE WHEN duedatetype = '-2147483648' 
THEN NULL ELSE duedatetype END) AS duedatetype,duedate,
(CASE WHEN numberofdays = '-2147483648' 
THEN NULL ELSE numberofdays END) AS numberofdays,automatedbillingstartdate, housingreq,
(CASE WHEN chkcancelwaiver = '128 ' 
THEN NULL ELSE chkcancelwaiver END) AS chkcancelwaiver,
(CASE WHEN noofseatbilled = '-2147483648' 
THEN NULL ELSE noofseatbilled END) AS noofseatbilled,
(CASE WHEN SessionTuitionCost = '-9.99999999999999E14' 
THEN NULL ELSE SessionTuitionCost END) AS SessionTuitionCost,Row_number() 
OVER(partition BY activity_pk ORDER BY lstupd DESC) AS row_num FROM mylearning_1720.tbl_tmx_activity where activity_pk>0""")
InsZ_TMP_TBL_TMX_Activity_Filtered_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_TMP_TBL_TMX_Activity_Filtered")

#Default null handling for InsZ_INT_tbl_tmx_activity and storing data to InsZ_INT_tbl_tmx_activity
InsZ_INT_tbl_tmx_activity_DF = spark.sql("""SELECT activity_pk ,activityname ,activitysearch ,activitylabelfk ,prntactfk ,code ,ccenter ,regexp_replace(activitydesc, '[\r\n]','') as activitydesc ,regexp_replace(activitydescfmt, '[\r\n]','') as activitydescfmt ,url ,active ,openforreg ,noregreqd ,maxattempts ,maxattemptsperparent ,cancelled ,hidden ,private ,regexp_replace(empnotes, '[\r\n]','') as empnotes ,regexp_replace(empnotesfmt, '[\r\n]','') as empnotesfmt ,regexp_replace(instrnotes, '[\r\n]','') as instrnotes ,regexp_replace(instrnotesfmt, '[\r\n]','') as instrnotesfmt ,isdeleted ,costbase ,costcncl ,costlatecncl ,costnoshow ,currencyfk ,paytermfk ,ecomreqd ,startdt ,enddt ,regdeadlinedt ,regcncldeadlinedt ,timezonefk ,useforconflictcheck ,reqappr ,defapprempfk ,ownerempfk ,regexp_replace(contact, '[\r\n]','') as contact ,reqpersonnel ,cbtlaunchmtdfk ,canbecopied ,canbesubscr ,canbeful ,rollupfromful ,stickyaudienceind ,copyoriginalactivity ,mincapacity ,maxcapacity ,recordsession ,requiredind ,pickrule ,pickruletype ,usetype ,restricted ,acttocomplete ,contributerollupind ,minpctgrd ,estdur ,estcrdhrs ,sendactcompletentf ,sendactreminderntf ,sendactregntf ,ordinal ,levelval ,rootactivityfk ,referenceactfk ,originalpk ,deffacfk ,hdnfrmtranscript ,tostagingdt ,lstupd ,usrname ,lck ,`timestamp`,regstartdt ,facultyprepstartdt ,facultyprependdt ,coursebillingcat ,createdt ,create_empfk ,fulfillcenterlearnermaterial ,fulfillcenterlearnerpostmaterial ,fulfillcenterinstructormaterial ,fulfillcenterinstructorpostmaterial ,shippingadd1 ,shippingadd2 ,shippingcity ,shippingstate ,shippingzip ,shippingcntry ,materialscontactname ,materialscontactemail ,materialscontactphn ,materialscontactfax ,courserating ,numberratings ,tuitiongraceperiod ,housinggraceperiod ,weeksregclose ,targetattendance ,transportationequalizedflag ,courseclassification ,managedregistrations ,travellogistics ,selfservice ,statutorytuitionrate ,allowmultisession ,coursemateriallink ,statuschangedt ,autoschedule ,allowinterested ,accessoncompletion ,isgenerictopic ,triggervtrform ,proficiencycode ,coursesponsor_fk ,activationdate ,curriculumpriority ,tuitionapproved ,tuitionapproveduser_fk ,tuitionapproveddt ,curriculumpreventveto ,curriculumdisplayintrainingsched ,curriculumdispcat ,readytobill ,readytobilluser_fk ,readytobilldt ,activity_sessionwbs_fk ,commuterchargeperday ,housingcostpernight ,centracompletioncriteria ,regexp_replace(activitydescenrolled, '[\r\n]','') as activitydescenrolled ,regexp_replace(activitydescenrolledfmt, '[\r\n]','') as activitydescenrolledfmt ,facultycommittedcapacity ,originaloccupancydaycommitment ,originalcapacity ,accessonequivalency ,waivers ,collapsecg ,canaccessvendorcontent ,coursecontentcontainsaccenture ,contentapproved ,areemployeesofaffiliates ,arecontractorspermitted ,seniorexecutivelevel ,statutorycostbase ,enableslfeatures ,searchtags ,coursearchtype ,applyacmi ,managedbymyc ,autoenrolllearners ,autoenrollstartdate ,createdbydst ,billingprocess ,billingevent ,licensetype ,recoverywbse ,allowlearnertocancel ,duedatetype ,duedate ,numberofdays ,automatedbillingstartdate ,housingreq ,chkcancelwaiver ,noofseatbilled ,sessiontuitioncost 
from myLearning_1720_tempdb.InsZ_TMP_TBL_TMX_Activity_Filtered where row_num=1""")
InsZ_INT_tbl_tmx_activity_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_tbl_tmx_activity")

#Default null handling for TBL_TMX_ACTLOC and storing data to InsZ_INT_TBL_TMX_ACTLOC
InsZ_TMP_TBL_TMX_ACTLOC_Filtered_DF = spark.sql("""
SELECT  activityfk, locfk, status, note, 
(CASE WHEN approverfk = '-2147483648' 
THEN NULL ELSE approverfk END) AS approverfk, lstupd, usrname, lck,Row_number() 
OVER(partition BY (activityfk, locfk) 
ORDER BY lstupd DESC) AS row_num 
FROM mylearning_1720.TBL_TMX_ACTLOC""")
InsZ_TMP_TBL_TMX_ACTLOC_Filtered_DF_Partitioned = InsZ_TMP_TBL_TMX_ACTLOC_Filtered_DF.repartition(InsZ_TMP_TBL_TMX_ACTLOC_Filtered_DF.row_num)
InsZ_TMP_TBL_TMX_ACTLOC_DF = InsZ_TMP_TBL_TMX_ACTLOC_Filtered_DF_Partitioned.filter(InsZ_TMP_TBL_TMX_ACTLOC_Filtered_DF.row_num==1)
InsZ_TMP_TBL_TMX_ACTLOC_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_TBL_TMX_ACTLOC")

#Default null handling for Tbl_Tmx_ActMetaData and storing data to InsZ_INT_Tbl_Tmx_ActMetaData
InsZ_TMP_Tbl_Tmx_ActMetaData_Filtered_DF = spark.sql("""
SELECT activityfk,
(CASE WHEN medtypefk = '-2147483648' 
THEN NULL ELSE medtypefk END) AS medtypefk,
(CASE WHEN contypefk = '-2147483648' 
THEN NULL ELSE contypefk END) AS contypefk,
(CASE WHEN lemtdfk = '-2147483648' 
THEN NULL ELSE lemtdfk END) AS lemtdfk, expirydt,
(CASE WHEN actstatfk = '-2147483648' 
THEN NULL ELSE actstatfk END) AS actstatfk,
(CASE WHEN langfk = '-2147483648' 
THEN NULL ELSE langfk END) AS langfk,
(CASE WHEN currencyfk = '-2147483648' 
THEN NULL ELSE currencyfk END) AS currencyfk,
(CASE WHEN regionfk = '-2147483648' 
THEN NULL ELSE regionfk END) AS regionfk, extns, extclass, extid, 
(CASE WHEN mdatafk = '-2147483648' 
THEN NULL ELSE mdatafk END) AS mdatafk, lstupd, usrname, lck, `timestamp`, cmr_cancelreason, cmr_cancelrequestor, cmr_canceldatecancelreq, 
(CASE WHEN delmtdfk = '-2147483648' 
THEN NULL ELSE delmtdfk END) AS delmtdfk, vendoractvityid, 
(CASE WHEN templatecode = '-2147483648' 
THEN NULL ELSE templatecode END) AS templatecode,
(CASE WHEN isd2lchecked = '-128 ' 
THEN NULL ELSE isd2lchecked END) AS isd2lchecked,
(CASE WHEN sesstypefk = '-2147483648' 
THEN NULL ELSE sesstypefk END) AS sesstypefk,
(CASE WHEN adobeinstanceneeded = '-128 ' 
THEN NULL ELSE adobeinstanceneeded END) AS adobeinstanceneeded, adobeinstancecreatedby, adobeinstancecreatedon, 
(CASE WHEN adobeinstancecreated  = '-128 ' 
THEN NULL ELSE adobeinstancecreated  END) AS adobeinstancecreated , actsubtype, 
(CASE WHEN onenrollment = '-2147483648' 
THEN NULL ELSE onenrollment END) AS onenrollment,
(CASE WHEN oncompletion = '-2147483648' 
THEN NULL ELSE oncompletion END) AS oncompletion, knowledgeadvisorcoursecode, aclceventid, aclc_sessiontype, Row_number() OVER(partition BY activityfk 
ORDER BY lstupd DESC) AS row_num 
FROM mylearning_1720.Tbl_Tmx_ActMetaData""")
InsZ_TMP_Tbl_Tmx_ActMetaData_Filtered_DF_Partitioned = InsZ_TMP_Tbl_Tmx_ActMetaData_Filtered_DF.repartition(InsZ_TMP_Tbl_Tmx_ActMetaData_Filtered_DF.row_num)
InsZ_TMP_Tbl_Tmx_ActMetaData_DF = InsZ_TMP_Tbl_Tmx_ActMetaData_Filtered_DF_Partitioned.filter(InsZ_TMP_Tbl_Tmx_ActMetaData_Filtered_DF.row_num==1)
InsZ_TMP_Tbl_Tmx_ActMetaData_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_Tbl_Tmx_ActMetaData")

#Default null handling for TBL_TMX_ActSkl and storing data to InsZ_INT_TBL_TMX_ActSkl
InsZ_INT_TBL_TMX_ActSkl_DF = spark.sql("""
SELECT activityfk, sklfk, 
(CASE WHEN successcriteria = '-2147483648' 
THEN NULL ELSE successcriteria END) AS successcriteria, 
(CASE WHEN grdfk = '-2147483648' 
THEN NULL ELSE grdfk END) AS grdfk, 
(CASE WHEN proffk = '-2147483648' 
THEN NULL ELSE proffk END) AS proffk,lstupd, usrname, lck,tagcategory 
FROM mylearning_1720.TBL_TMX_ActSkl""")
InsZ_INT_TBL_TMX_ActSkl_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_TBL_TMX_ActSkl")

#Default null handling for InsZ_TMP_Tbl_Tmx_EmpActApproval_MaxFilter and storing data to InsZ_TMP_Tbl_Tmx_EmpActApproval_MaxFilter
InsZ_TMP_Tbl_Tmx_EmpActApproval_MaxFilter_DF =  spark.sql("""SELECT MAX(EmpActApproval_PK) AS EmpActApproval_PK  
FROM mylearning_1720.Tbl_Tmx_EmpActApproval 
GROUP BY RegistrationFK, EmpFK HAVING COUNT(*) > 1 
UNION SELECT MAX(EmpActApproval_PK) AS EmpActApproval_PK 
FROM mylearning_1720.Tbl_Tmx_EmpActApproval 
GROUP BY RegistrationFK, EmpFK 
HAVING COUNT(*) = 1""")
InsZ_TMP_Tbl_Tmx_EmpActApproval_MaxFilter_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_TMP_Tbl_Tmx_EmpActApproval_MaxFilter")

#Default null handling for InsZ_TMP_Tbl_Tmx_EmpActApproval_Filtered and storing data to InsZ_TMP_Tbl_Tmx_EmpActApproval_Filtered
InsZ_TMP_Tbl_Tmx_EmpActApproval_Filtered_DF = spark.sql("""
SELECT APPR.EmpActApproval_PK,APPR.RegistrationFK, APPR.EmpFK, 
(CASE WHEN APPR.ActivityFK = '-2147483648' 
THEN NULL ELSE ActivityFK END) AS ActivityFK, APPR.TrackFK, 
(CASE WHEN APPR.ApproverFK = '-2147483648' 
THEN NULL ELSE ApproverFK END) AS ApproverFK , APPR.Status, APPR.ApprType, APPR.ActionDt, 
(CASE WHEN APPR.ActualApproverFK = '-2147483648' 
THEN NULL ELSE APPR.ActualApproverFK END) AS ActualApproverFK , 
(CASE WHEN APPR.Ordinal = '-2147483648' 
THEN NULL ELSE APPR.Ordinal END) AS Ordinal, APPR.ApprNotes, APPR.LstUpd, APPR.UsrName, APPR.Lck, 
(CASE WHEN APPR.ForwardedApproverFK = '-2147483648' 
THEN NULL ELSE APPR.ForwardedApproverFK END) AS ForwardedApproverFK, APPR.ForwardedDt, 
(CASE WHEN APPR.Recommendation = '-2147483648' 
THEN NULL ELSE APPR.Recommendation END) AS Recommendation, APPR.RecommendationNt, APPR.Urgent, 
TRIM(APPR.requesttype) AS requesttype,Row_number() 
OVER(partition BY APPR.empactapproval_pk 
ORDER BY APPR.lstupd DESC) AS row_num 
FROM mylearning_1720.Tbl_Tmx_EmpActApproval APPR 
Inner JOIN mylearning_1720_tempdb.InsZ_TMP_Tbl_Tmx_EmpActApproval_MaxFilter ApprMin 
ON APPR.EmpActApproval_PK = ApprMin.EmpActApproval_PK
""")
InsZ_TMP_Tbl_Tmx_EmpActApproval_Filtered_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_TMP_Tbl_Tmx_EmpActApproval_Filtered")

#Default null handling for InsZ_INT_Tbl_Tmx_EmpActApproval and storing data to InsZ_INT_Tbl_Tmx_EmpActApproval
InsZ_INT_Tbl_Tmx_EmpActApproval_DF =  spark.sql("""SELECT empactapproval_pk ,registrationfk ,empfk ,activityfk ,trackfk ,approverfk ,status ,apprtype ,actiondt ,actualapproverfk ,ordinal ,regexp_replace(apprnotes, '[\r\n]','') as apprnotes ,lstupd ,usrname ,lck ,forwardedapproverfk ,forwardeddt ,recommendation ,regexp_replace(recommendationnt, '[\r\n]','') as recommendationnt ,urgent ,requesttype 
from myLearning_1720_tempdb.InsZ_TMP_Tbl_Tmx_EmpActApproval_Filtered 
where row_num=1""")
InsZ_INT_Tbl_Tmx_EmpActApproval_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_Tbl_Tmx_EmpActApproval")

#Default null handling for TBLEmp by creating views InsZ_TMP_TBLEmp1_Filtered,InsZ_TMP_TBLEmp2_Filtered_Insz and storing data to InsZ_INT_TBLEmp
InsZ_TMP_TBLEmp1_Filtered_DF = spark.sql(""" 
SELECT emp_pk, 
(CASE WHEN emp_empcdfk = '-2147483648' 
THEN NULL ELSE emp_empcdfk END) AS emp_empcdfk, 
(CASE WHEN emp_empstatfk = '-2147483648' 
THEN NULL ELSE emp_empstatfk END) AS emp_empstatfk, 
(CASE WHEN emp_mgrempfk = '-2147483648' 
THEN NULL ELSE emp_mgrempfk END) AS emp_mgrempfk, 
(CASE WHEN emp_level = '-2147483648' 
THEN NULL ELSE emp_level END) AS emp_level,emp_no,emp_lname,emp_fname,emp_lnamepr,emp_fnamepr,emp_title,emp_mi,emp_fullname,emp_suffix,emp_ttl,emp_startdt,emp_enddt, emp_internalind,emp_add1,emp_add2,emp_city,emp_state,emp_cntry,emp_zip,emp_email,emp_phn1,emp_phn2,emp_fax,emp_url,emp_note,emp_lstupd,emp_usrname, emp_lck, (CASE WHEN emp_qualtoappr = '-2147483648' 
THEN NULL ELSE emp_qualtoappr END) AS emp_qualtoappr, 
(CASE WHEN emp_defapprempfk = '-2147483648' 
THEN NULL ELSE emp_defapprempfk END) AS emp_defapprempfk, 
(CASE WHEN emp_apprempfk = '-2147483648' 
THEN NULL ELSE emp_apprempfk END) AS emp_apprempfk, 
(CASE WHEN emp_reqconfirm = '-2147483648' 
THEN NULL ELSE emp_reqconfirm END) AS emp_reqconfirm,emp_extns,emp_extclass,emp_extid,emp_timestamp,emp_lstrvwdt, 
(CASE WHEN emp_active = '-2147483648' 
THEN NULL ELSE emp_active END) AS emp_active, emp_isdeleted,emp_companycode, emp_monthsatlevel, emp_yearsatlevel,emp_experiencedhire, 
(CASE WHEN emp_substatus_fk = '-2147483648' 
THEN NULL ELSE emp_substatus_fk END) AS emp_substatus_fk,emp_gpspersonnelid, 
(CASE WHEN emp_peoplekey = '-2147483648' 
THEN NULL ELSE emp_peoplekey END) AS emp_peoplekey,emp_lastpromotiondate, 
(CASE WHEN emp_loc_fk = '-2147483648' 
THEN NULL ELSE emp_loc_fk END) AS emp_loc_fk,emp_gpsresponsibletype, 
(CASE WHEN emp_hrrepemp_fk = '-2147483648' 
THEN NULL ELSE emp_hrrepemp_fk END) AS emp_hrrepemp_fk,emp_smokerflag, 
(CASE WHEN emp_careercounseloremp_fk = '-2147483648' 
THEN NULL ELSE emp_careercounseloremp_fk END) AS emp_careercounseloremp_fk,
(CASE WHEN emp_actualtraininghours = '-9.99999999999999E14' 
THEN NULL ELSE emp_actualtraininghours END) AS emp_actualtraininghours, 
(CASE WHEN emp_actualtrainingdnp = '-2147483648' 
THEN NULL ELSE emp_actualtrainingdnp END) AS emp_actualtrainingdnp, 
(CASE WHEN emp_budgettraininghours = '-2147483648' 
THEN NULL ELSE emp_budgettraininghours END) AS emp_budgettraininghours, 
(CASE WHEN emp_budgettrainingdnp = '-2147483648' 
THEN NULL ELSE emp_budgettrainingdnp END) AS emp_budgettrainingdnp,emp_alltrainingreqappr,emp_orgchangeflag,emp_totalestimatedflightcost,emp_totalestimatedhousingcost,emp_newlyinactivets,emp_autocancelwarningsent, 
(CASE WHEN emp_additionalcareercounselor1_fk = '-2147483648' 
THEN NULL ELSE emp_additionalcareercounselor1_fk END) AS emp_additionalcareercounselor1_fk, 
(CASE WHEN emp_additionalcareercounselor2_fk = '-2147483648' 
THEN NULL ELSE emp_additionalcareercounselor2_fk END) AS emp_additionalcareercounselor2_fk, 
(CASE WHEN emp_profitcenter_fk = '-2147483648' 
THEN NULL ELSE emp_profitcenter_fk END) AS emp_profitcenter_fk, 
(CASE WHEN emp_costcenter_fk = '-2147483648' 
THEN NULL ELSE emp_costcenter_fk END) AS emp_costcenter_fk, 
(CASE WHEN emp_docentuserid = '-2147483648' 
THEN NULL ELSE emp_docentuserid END) AS emp_docentuserid,emp_displayreqintrainingsched,emp_updatecurriculumlevel, 
(CASE WHEN emp_vendorfk = '-2147483648' 
THEN NULL ELSE emp_vendorfk END) AS emp_vendorfk,emp_preferredname, 
(CASE WHEN emp_preferrednameoverrideflag = '-128' 
THEN NULL ELSE emp_preferrednameoverrideflag END) AS emp_preferrednameoverrideflag, 
(CASE WHEN emp_secapprreq = '-2147483648' 
THEN NULL ELSE emp_secapprreq END) AS emp_secapprreq,orgunitid,orgunitabbr,orgunitdescr,emp_employmentstatuscd,emp_aliasemail, 
(CASE WHEN emp_canaccessreporting = '-2147483648' 
THEN NULL ELSE emp_canaccessreporting END) AS emp_canaccessreporting, 
(CASE WHEN acn_mlenabled = '-128' 
THEN NULL ELSE acn_mlenabled END) AS acn_mlenabled,acn_mldtime,acn_mldtimeorig, 
(CASE WHEN emp_secondaryapprempfk = '-2147483648' 
THEN NULL ELSE emp_secondaryapprempfk END) AS emp_secondaryapprempfk, 
(CASE WHEN emp_isavanade = '-128' 
THEN NULL ELSE emp_isavanade END) AS emp_isavanade,candidateid,emp_createdby,emp_createddate,nationality,Row_number() 
OVER(partition BY emp_pk ORDER BY emp_lstupd DESC) AS row_num 
FROM mylearning_1720.TBLEmp""")
InsZ_TMP_TBLEmp1_Filtered_view = InsZ_TMP_TBLEmp1_Filtered_DF.createTempView("InsZ_TMP_TBLEmp1_Filtered")
InsZ_TMP_TBLEmp2_Filtered_Insz_DF = spark.sql(""" SELECT emp_pk,  emp_empcdfk,  emp_empstatfk,  emp_mgrempfk,  emp_level,emp_no,emp_lname,emp_fname,emp_lnamepr,emp_fnamepr,emp_title,emp_mi,emp_fullname,emp_suffix,emp_ttl,emp_startdt,emp_enddt, emp_internalind,emp_add1,emp_add2,emp_city,emp_state,emp_cntry,emp_zip,emp_email,emp_phn1,emp_phn2,emp_fax,emp_url,emp_note,emp_lstupd,emp_usrname, emp_lck, emp_qualtoappr, emp_defapprempfk,  emp_apprempfk,  emp_reqconfirm,emp_extns,emp_extclass,emp_extid,emp_timestamp,emp_lstrvwdt, emp_active, emp_isdeleted,emp_companycode, emp_monthsatlevel, emp_yearsatlevel,emp_experiencedhire,  emp_substatus_fk,emp_gpspersonnelid,  emp_peoplekey,emp_lastpromotiondate,  emp_loc_fk,emp_gpsresponsibletype, emp_hrrepemp_fk,emp_smokerflag,  emp_careercounseloremp_fk, emp_actualtraininghours, emp_actualtrainingdnp,  emp_budgettraininghours,  emp_budgettrainingdnp,emp_alltrainingreqappr,emp_orgchangeflag,emp_totalestimatedflightcost,emp_totalestimatedhousingcost,emp_newlyinactivets,emp_autocancelwarningsent,  emp_additionalcareercounselor1_fk,  emp_additionalcareercounselor2_fk,  emp_profitcenter_fk,  emp_costcenter_fk,  emp_docentuserid,emp_displayreqintrainingsched,emp_updatecurriculumlevel, emp_vendorfk,emp_preferredname, emp_preferrednameoverrideflag,  emp_secapprreq,orgunitid,orgunitabbr,orgunitdescr,emp_employmentstatuscd,emp_aliasemail,emp_canaccessreporting,acn_mlenabled,acn_mldtime,acn_mldtimeorig, emp_secondaryapprempfk, emp_isavanade,candidateid,emp_createdby,emp_createddate,nationality ,ROW_NUMBER() 
OVER(PARTITION BY emp_peoplekey ORDER BY emp_pk DESC) as rnm 
FROM InsZ_TMP_TBLEmp1_Filtered where row_num=1""")
InsZ_TMP_TBLEmp2_Filtered_Insz_view = InsZ_TMP_TBLEmp2_Filtered_Insz_DF.createTempView("InsZ_TMP_TBLEmp2_Filtered_Insz")
InsZ_INT_TBLEMP_DF = spark.sql(""" SELECT emp_pk,  emp_empcdfk,  emp_empstatfk,  emp_mgrempfk,  emp_level,emp_no,emp_lname,emp_fname,emp_lnamepr,emp_fnamepr,emp_title,emp_mi,emp_fullname,emp_suffix,emp_ttl,emp_startdt,emp_enddt, emp_internalind,emp_add1,emp_add2,emp_city,emp_state,emp_cntry,emp_zip,emp_email,emp_phn1,emp_phn2,emp_fax,emp_url,emp_note,emp_lstupd,emp_usrname, emp_lck, emp_qualtoappr, emp_defapprempfk,  emp_apprempfk,  emp_reqconfirm,emp_extns,emp_extclass,emp_extid,emp_timestamp,emp_lstrvwdt, emp_active, emp_isdeleted,emp_companycode, emp_monthsatlevel, emp_yearsatlevel,emp_experiencedhire,  emp_substatus_fk,emp_gpspersonnelid,  emp_peoplekey,emp_lastpromotiondate,  emp_loc_fk,emp_gpsresponsibletype, emp_hrrepemp_fk,emp_smokerflag,  emp_careercounseloremp_fk, emp_actualtraininghours, emp_actualtrainingdnp,  emp_budgettraininghours,  emp_budgettrainingdnp,emp_alltrainingreqappr,emp_orgchangeflag,emp_totalestimatedflightcost,emp_totalestimatedhousingcost,emp_newlyinactivets,emp_autocancelwarningsent,  emp_additionalcareercounselor1_fk,  emp_additionalcareercounselor2_fk,  emp_profitcenter_fk,  emp_costcenter_fk,  emp_docentuserid,emp_displayreqintrainingsched,emp_updatecurriculumlevel, emp_vendorfk,emp_preferredname, emp_preferrednameoverrideflag,  emp_secapprreq,orgunitid,orgunitabbr,orgunitdescr,emp_employmentstatuscd,emp_aliasemail,emp_canaccessreporting,acn_mlenabled,acn_mldtime,acn_mldtimeorig, emp_secondaryapprempfk, emp_isavanade,candidateid,emp_createdby,emp_createddate,nationality 
FROM InsZ_TMP_TBLEmp2_Filtered_Insz where rnm=1""")
InsZ_INT_TBLEMP_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_TBLEmp")

#Default null handling for tblEmpOrg and storing data to InsZ_INT_tblEmpOrg
InsZ_TMP_tblEmpOrg_Filtered_DF = spark.sql("""
SELECT emporg_empfk,emporg_orgfk, emporg_prmyind,emporg_joiningdate,emporg_lstupd,emporg_usrname, emporg_lck, 
(CASE WHEN emporg_additionalind = '-32768' 
THEN NULL ELSE emporg_additionalind END) AS emporg_additionalind,
(CASE WHEN proficiencycdfk = '-2147483648' 
THEN NULL ELSE proficiencycdfk END) AS proficiencycdfk,
(CASE WHEN assessmentcdfk = '-2147483648' 
THEN NULL ELSE assessmentcdfk END) AS assessmentcdfk,
(CASE WHEN proficiencyscalecd = '-2147483648' 
THEN NULL ELSE proficiencyscalecd END) AS proficiencyscalecd, 
(CASE WHEN scaleid = '-2147483648' 
THEN NULL ELSE scaleid END) AS scaleid, 
(CASE WHEN proficiencyupdatesource = '-128' 
THEN NULL ELSE proficiencyupdatesource END) AS proficiencyupdatesource,Row_number() 
OVER(partition BY (emporg_empfk ,emporg_orgfk) 
ORDER BY emporg_lstupd DESC) AS row_num 
FROM myLearning_1720.tblEmpOrg""")
InsZ_TMP_tblEmpOrg_Filtered_DF_Partitioned = InsZ_TMP_tblEmpOrg_Filtered_DF.repartition(InsZ_TMP_tblEmpOrg_Filtered_DF.row_num)
InsZ_TMP_tblEmpOrg_DF = InsZ_TMP_tblEmpOrg_Filtered_DF_Partitioned.filter(InsZ_TMP_tblEmpOrg_Filtered_DF.row_num==1)
InsZ_TMP_tblEmpOrg_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_tblEmpOrg")

#Default null handling for Ven and storing data to InsZ_INT_Ven
InsZ_INT_Ven_DF = spark.sql("""
SELECT ven_pk, ven_name, ven_ttl, ven_prefind, ven_fname, ven_lname, ven_add1, ven_add2, ven_city, ven_state, ven_cntry, ven_zip, ven_email, ven_phn1, ven_phn2, ven_fax, ven_url, ven_reqpersonnel, ven_note, ven_internalind, ven_costind, ven_lstupd, ven_usrname, ven_timestamp, ven_lck, ven_creationdate, ven_codeneededcd, ven_triggervendorcd, ven_statuscd, 
(CASE WHEN ven_curriculumcontact_fk = '-2147483648' 
THEN NULL ELSE ven_curriculumcontact_fk END) AS ven_curriculumcontact_fk,
(CASE WHEN ven_alliancecontact_fk = '-2147483648' 
THEN NULL ELSE ven_alliancecontact_fk END) AS ven_alliancecontact_fk, ven_curriculumurl, regexp_replace(ven_vendordiscountinfo, '[\r\n]','') as ven_vendordiscountinfo, regexp_replace(ven_vendorreginstructions, '[\r\n]','') as ven_vendorreginstructions, 
(CASE WHEN ven_country_fk = '-2147483648' 
THEN NULL ELSE ven_country_fk END) AS ven_country_fk,
(CASE WHEN ven_docentid = '-2147483648' 
THEN NULL ELSE ven_docentid END) AS ven_docentid, ven_enteredby, ven_updatedby 
FROM mylearning_1720.Ven""")
InsZ_INT_Ven_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_Ven")

#Default null handling for tbl_tax_node and storing data to InsZ_INT_tbl_tax_node
InsZ_INT_tbl_tax_node_DF = spark.sql("""
SELECT nodeidpk,
(CASE WHEN prntnodeidfk = '-2147483648' 
THEN NULL ELSE prntnodeidfk END) AS prntnodeidfk,
(CASE WHEN ordinal = '-2147483648' 
THEN NULL ELSE ordinal END) AS ordinal,
(CASE WHEN nodelevel = '-2147483648' 
THEN NULL ELSE nodelevel END) AS nodelevel,nodename,nodedesc,nodekeywords,taxstateidfk,sysind,sysclass,sysid,lstupd,username,updatetimestamp 
FROM mylearning_1720.tbl_tax_node  """)
InsZ_INT_tbl_tax_node_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_tbl_tax_node")

#Default null handling for tbl_tax_item and storing data to InsZ_INT_tbl_tax_item
InsZ_TMP_tbl_tax_item_Filtered_DF = spark.sql("""
SELECT itemid_pk,nodeid_fk,
(CASE WHEN contypeid_fk = '-2147483648' 
THEN NULL ELSE contypeid_fk END) AS contypeid_fk,extstrid,
(CASE WHEN extintid = '-2147483648' 
THEN NULL ELSE extintid END) AS extintid,lstupd,username,Row_number() 
OVER(partition BY itemid_pk 
ORDER BY lstupd DESC) AS row_num 
FROM mylearning_1720.tbl_tax_item""")
InsZ_TMP_tbl_tax_item_Filtered_DF_Partitioned = InsZ_TMP_tbl_tax_item_Filtered_DF.repartition(InsZ_TMP_tbl_tax_item_Filtered_DF.row_num)
InsZ_TMP_tbl_tax_item_DF = InsZ_TMP_tbl_tax_item_Filtered_DF_Partitioned.filter(InsZ_TMP_tbl_tax_item_Filtered_DF.row_num==1)
InsZ_TMP_tbl_tax_item_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_tbl_tax_item")

#Default null handling for TBL_CPT_PlanYear and storing data to InsZ_INT_TBL_CPT_PlanYear
InsZ_INT_TBL_CPT_PlanYear_DF = spark.sql("""
SELECT planyearpk,code,name,lstupddt,lstuptby,
(CASE WHEN phase = '-2147483648' 
THEN NULL ELSE phase END) AS phase,createddt,
(CASE WHEN discretionaryphase = '-2147483648' 
THEN NULL ELSE discretionaryphase END) AS discretionaryphase 
FROM mylearning_1720.TBL_CPT_PlanYear""")
InsZ_INT_TBL_CPT_PlanYear_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_TBL_CPT_PlanYear")

#Commenting these as we are not using these tables.
'''
InsZ_TMP_Past_DF = spark.sql("""
SELECT PeopleKey 
FROM mrdr_1033.PeopleAll 
WHERE sapeffectivedt < current_date()""")
InsZ_TMP_Past_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_Past")
InsZ_TMP_Future_DF = spark.sql("""
SELECT PeopleKey 
FROM mrdr_1033.PeopleAll 
WHERE sapeffectivedt > current_date()""")
InsZ_TMP_Future_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_Future")
InsZ_TMP_Past_SAPEft_DF = spark.sql("""
SELECT P.PeopleKey,MAX(PA.sapeffectivedt) as sapeffectivedt 
FROM mylearning_1720.InsZ_TMP_Past P 
INNER JOIN mrdr_1033.PeopleAll PA on P.peoplekey = PA.peoplekey 
LEFT JOIN mylearning_1720.InsZ_TMP_Future F ON P.PeopleKey = F.PeopleKey 
WHERE F.PeopleKey IS NULL 
GROUP BY P.PeopleKey""")
InsZ_TMP_Past_SAPEft_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_Past_SAPEft")
InsZ_TMP_Mid_SAPEft_DF = spark.sql("""
SELECT F.PeopleKey,MIN(PA.sapeffectivedt) as sapeffectivedt 
FROM mylearning_1720.InsZ_TMP_Past P 
INNER JOIN mylearning_1720.InsZ_TMP_Future F ON P.PeopleKey = F.PeopleKey 
INNER JOIN mrdr_1033.PeopleAll PA on P.peoplekey = PA.peoplekey 
GROUP BY F.PeopleKey""")
InsZ_TMP_Mid_SAPEft_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_Mid_SAPEft")
InsZ_TMP_Future_SAPEft_DF = spark.sql("""
SELECT F.PeopleKey,MIN(PA.sapeffectivedt) as sapeffectivedt 
FROM mylearning_1720.InsZ_TMP_Future F 
INNER JOIN mrdr_1033.PeopleAll PA on F.peoplekey = PA.peoplekey 
LEFT JOIN mylearning_1720.InsZ_TMP_Past P ON F.PeopleKey = P.PeopleKey 
WHERE P.PeopleKey IS NULL 
GROUP BY F.PeopleKey""")
InsZ_TMP_Future_SAPEft_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_Future_SAPEft")
InsZ_TMP_Past_PeopleAll_DF = spark.sql("""
SELECT PA.peoplekey,PA.lastnm,PA.firstnm,PA.EmploymentStatusCd,PA.loastatus,PA.EmployeeSubgroupCd,PA.Countrynm,PA.personnelnbr,PA.internetmail,PA.profitcenternbr,PA.costcenternbr,PA.metrocitydescr,PA.careercounselorid,PA.orgunitabbr 
FROM mylearning_1720.InsZ_TMP_Past_SAPEft PS 
INNER JOIN mrdr_1033.PeopleAll PA ON PS.PeopleKey = PA.PeopleKey AND PS.sapeffectivedt = PA.sapeffectivedt 
WHERE NVL(PA.personnelnbr,'') <> ''""")
InsZ_TMP_Past_PeopleAll_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_Past_PeopleAll")
InsZ_TMP_Mid_PeopleAll_DF = spark.sql("""
SELECT PA.peoplekey,PA.lastnm,PA.firstnm,PA.EmploymentStatusCd,PA.loastatus,PA.EmployeeSubgroupCd,PA.Countrynm,PA.personnelnbr,PA.internetmail,PA.profitcenternbr,PA.costcenternbr,PA.metrocitydescr,PA.careercounselorid,PA.orgunitabbr 
FROM mylearning_1720.InsZ_TMP_Mid_SAPEft MS 
INNER JOIN mrdr_1033.PeopleAll PA ON MS.PeopleKey = PA.PeopleKey AND MS.sapeffectivedt = PA.sapeffectivedt 
WHERE NVL(PA.personnelnbr,'') <> ''""")
InsZ_TMP_Mid_PeopleAll_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_Mid_PeopleAll")
InsZ_TMP_Future_PeopleAll_DF = spark.sql("""
SELECT PA.peoplekey,PA.lastnm,PA.firstnm,PA.EmploymentStatusCd,PA.loastatus,PA.EmployeeSubgroupCd,PA.Countrynm,PA.personnelnbr,PA.internetmail,PA.profitcenternbr,PA.costcenternbr,PA.metrocitydescr,PA.careercounselorid,PA.orgunitabbr 
FROM mylearning_1720.InsZ_TMP_Future_SAPEft FS 
INNER JOIN mrdr_1033.PeopleAll PA ON FS.PeopleKey = PA.PeopleKey AND FS.sapeffectivedt = PA.sapeffectivedt 
WHERE NVL(PA.personnelnbr,'') <> ''""")
InsZ_TMP_Future_PeopleAll_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_Future_PeopleAll")
InsZ_INT_PeopleAll_DF = spark.sql("""
SELECT Pa.peoplekey,Pa.lastnm,Pa.firstnm,Pa.EmploymentStatusCd,Pa.loastatus,Pa.EmployeeSubgroupCd,Pa.Countrynm,Pa.personnelnbr,Pa.internetmail,Pa.profitcenternbr,Pa.costcenternbr,Pa.metrocitydescr,Pa.careercounselorid,Pa.orgunitabbr 
FROM mylearning_1720.InsZ_TMP_Past_PeopleAll Pa 
UNION SELECT Mi.peoplekey,Mi.lastnm,Mi.firstnm,Mi.EmploymentStatusCd,Mi.loastatus,Mi.EmployeeSubgroupCd,Mi.Countrynm,Mi.personnelnbr,Mi.internetmail,Mi.profitcenternbr,Mi.costcenternbr,Mi.metrocitydescr,Mi.careercounselorid,Mi.orgunitabbr 
FROM mylearning_1720.InsZ_TMP_Mid_PeopleAll Mi 
UNION SELECT Fu.peoplekey,Fu.lastnm,Fu.firstnm,Fu.EmploymentStatusCd,Fu.loastatus,Fu.EmployeeSubgroupCd,Fu.Countrynm,Fu.personnelnbr,Fu.internetmail,Fu.profitcenternbr,Fu.costcenternbr,Fu.metrocitydescr,Fu.careercounselorid,Fu.orgunitabbr 
FROM mylearning_1720.InsZ_TMP_Future_PeopleAll Fu""")
InsZ_INT_PeopleAll_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_PeopleAll")
'''

#Default null handling for TBL_TMX_RegBatch and storing data to InsZ_INT_TBL_TMX_RegBatch
InsZ_TMP_TBL_TMX_RegBatch_Filtered_DF = spark.sql("""
SELECT regbatch_pk,enrollerfk,
(case when t1.trackfk = '-2147483648' 
THEN NULL else t1.trackfk end) as trackfk,creationtime,
(case when t1.batchstate = '-2147483648' 
THEN NULL else t1.batchstate end) as batchstate,expireafter,lstupd,usrname,lck,Row_number() 
OVER(partition BY t1.regbatch_pk 
ORDER BY t1.LstUpd DESC) AS row_num 
FROM myLearning_1720.TBL_TMX_RegBatch t1""")
InsZ_TMP_TBL_TMX_RegBatch_Filtered_DF_Partitioned = InsZ_TMP_TBL_TMX_RegBatch_Filtered_DF.repartition(InsZ_TMP_TBL_TMX_RegBatch_Filtered_DF.row_num)
InsZ_TMP_TBL_TMX_RegBatch_DF = InsZ_TMP_TBL_TMX_RegBatch_Filtered_DF_Partitioned.filter(InsZ_TMP_TBL_TMX_RegBatch_Filtered_DF.row_num==1)
InsZ_TMP_TBL_TMX_RegBatch_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_TBL_TMX_RegBatch")

#Default null handling for TBL_TMX_Attempt and storing data to InsZ_INT_TBL_TMX_Attempt
InsZ_TMP_TBL_TMX_Attempt_Filtered_DF = spark.sql("""
SELECT t1.Attempt_PK,t1.EmpFK,t1.ActivityFK,
(case when t1.AuthRegFK = '-2147483648' 
THEN NULL else t1.AuthRegFK end) as AuthRegFK,t1.StartDt,t1.EndDt,t1.WaiveInd,
(case when t1.CompletionStatus = '-2147483648' 
THEN NULL else t1.CompletionStatus end) as CompletionStatus,
(case when  t1.Success = '-2147483648' 
THEN NULL else t1.Success end) as Success,
(CASE WHEN Score = '-999999999999999' 
THEN NULL ELSE Score END) AS Score,
(case when t1.AttndStatusFK = '-2147483648' 
THEN NULL else t1.AttndStatusFK end) as AttndStatusFK,
(case when t1.GrdFK = '-2147483648' 
THEN NULL else t1.GrdFK end) as GrdFK,t1.Note,
(case when t1.ElapsedSeconds = '-2147483648' 
THEN NULL else t1.ElapsedSeconds end) as ElapsedSeconds,LaunchCourseVersion,
(case when t1.MergedSkl = '-2147483648' 
THEN NULL else t1.MergedSkl end) as MergedSkl,
(case when t1.CurrentAttemptInd = '-2147483648' 
THEN NULL else t1.CurrentAttemptInd end) as CurrentAttemptInd,t1.LaunchCount,
(case when t1.LinkFK = '-2147483648' 
THEN NULL else t1.LinkFK end) as LinkFK,
(case when t1.Source = '-2147483648' 
THEN NULL else t1.Source end) as Source,t1.SessionId,t1.ExpirationDate,t1.ReplanDate,
(case when t1.ExpirationType = '-2147483648' 
THEN NULL else t1.ExpirationType end) as ExpirationType,
(case when t1.ModifierFK = '-2147483648' 
THEN NULL else t1.ModifierFK end) as ModifierFK,t1.CertNotes,
(case when t1.wasActivation = '-128' 
THEN NULL else t1.wasActivation end) as wasActivation,
(case when t1.OriginalEvtFK = '-2147483648' 
THEN NULL else t1.OriginalEvtFK end) as OriginalEvtFK,
(case when t1.OfflineInd = '-2147483648' 
THEN NULL else t1.OfflineInd end) as OfflineInd,t1.LastSyncDt,t1.LstUpd,t1.UsrName,t1.Lck,
(case when t1.WebExRegisterId = '-2147483648' 
THEN NULL else t1.WebExRegisterId end) as WebExRegisterId,
(case when t1.WebExAttendeeId = '-2147483648' 
THEN NULL else t1.WebExAttendeeId end) as WebExAttendeeId,t1.LaunchUrl,t1.DesignationStatus,t1.DesignationUpDt,t1.CertExpirationDate,Row_number() 
OVER(partition BY t1.Attempt_PK 
ORDER BY t1.LstUpd DESC) AS row_num 
FROM myLearning_1720.TBL_TMX_Attempt t1 """)
InsZ_TMP_TBL_TMX_Attempt_Filtered_DF_Partitioned = InsZ_TMP_TBL_TMX_Attempt_Filtered_DF.repartition(InsZ_TMP_TBL_TMX_Attempt_Filtered_DF.row_num)
InsZ_TMP_TBL_TMX_Attempt_DF = InsZ_TMP_TBL_TMX_Attempt_Filtered_DF_Partitioned.filter(InsZ_TMP_TBL_TMX_Attempt_Filtered_DF.row_num==1)
InsZ_TMP_TBL_TMX_Attempt_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_TBL_TMX_Attempt")

#Default null handling for InsZ_TMP_TBL_TMX_Registration_Filtered and storing data to InsZ_TMP_TBL_TMX_Registration_Filtered
InsZ_TMP_TBL_TMX_Registration_Filtered_DF = spark.sql("""SELECT t1.Reg_PK,t1.EmpFK,t1.ActivityFK,t1.Status,
(CASE when t1.MaxAttempts = '-2147483648' 
THEN NULL else t1.MaxAttempts end) as MaxAttempts,
(CASE when t1.AllocationFK = '-2147483648' 
THEN NULL else t1.AllocationFK end) as AllocationFK,t1.CancellationDateTime,t1.RmndNoteDt,t1.CnfmNoteDt,t1.CnclNoteDt,t1.CertNoteDt,t1.ExtNS,t1.ExtClass,t1.ExtID,t1.ValidationKey,t1.LocalPath,t1.LstUpd,t1.UsrName,t1.Lck,
(CASE WHEN courseHours = '-999999999999999' 
THEN NULL ELSE courseHours END) AS courseHours,
(CASE WHEN estimatedTuition= '-9.99999999999999E14' 
THEN NULL ELSE estimatedTuition END) AS estimatedTuition,
(CASE WHEN estimatedTravelCost = '-9.99999999999999E14' 
THEN NULL ELSE estimatedTravelCost END) AS estimatedTravelCost ,
(CASE WHEN estimatedHousingCost = '-9.99999999999999E14' 
THEN NULL ELSE estimatedHousingCost END) AS estimatedHousingCost,TRIM(t1.cancelReasonCd) AS cancelReasonCd,t1.cancelReasonText,t1.cancelBillCd,t1.cancelInitTS,TRIM(t1.waivedReasonCd) AS waivedReasonCd,t1.waivedReasonTxt,t1.extIntegTxnType,
(CASE when t1.curriculumPriority = '-2147483648' 
THEN NULL else t1.curriculumPriority end) as curriculumPriority,
(CASE when t1.flightType = '-2147483648' 
THEN NULL else t1.flightType end) as flightType,
(CASE when t1.WBS_FK = '-2147483648' 
THEN NULL else t1.WBS_FK end) as WBS_FK,t1.assetsOrdered,t1.partTimeFaculty,t1.srtSource,t1.srtCategory,t1.srtLocationName,t1.estimatedTuitionOverride,t1.completedSurvey,t1.main,
(CASE when t1.docentID = '-2147483648' 
THEN NULL else t1.docentID end) as docentID,
(CASE when t1.CertSponsor2ID = '-2147483648' 
THEN NULL else t1.CertSponsor2ID end) as CertSponsor2ID,
(CASE when t1.CertSponsor1ID = '-2147483648' 
THEN NULL else t1.CertSponsor1ID end) as CertSponsor1ID,
(CASE when t1.Interviewer_EmpFK = '-2147483648' 
THEN NULL else t1.Interviewer_EmpFK end) as Interviewer_EmpFK,
(CASE when t1.Interview_Schedule = '-128' 
THEN NULL else t1.Interview_Schedule end) as Interview_Schedule,
(CASE when t1.CancellationRequested = '-2147483648' 
THEN NULL else t1.CancellationRequested end) as CancellationRequested,Row_number() 
OVER(partition BY t1.reg_pk ORDER BY t1.LstUpd DESC) AS row_num 
FROM myLearning_1720.TBL_TMX_Registration t1""")
InsZ_TMP_TBL_TMX_Registration_Filtered_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_TMP_TBL_TMX_Registration_Filtered")

#Default null handling for TBL_TMX_RegistrationTrack and storing data to InsZ_INT_TBL_TMX_RegistrationTrack
InsZ_TMP_TBL_TMX_RegistrationTrack_Filtered_DF = spark.sql("""
SELECT t1.RegTrack_PK,t1.RegFK,
(case when t1.TrackFK = '-2147483648' 
THEN NULL else t1.TrackFK end) as TrackFK,
(case when t1.EnrollerFK = '-2147483648' 
THEN NULL else t1.EnrollerFK end) as EnrollerFK,
(case when t1.RegXmlFK = '-2147483648' 
THEN NULL else t1.RegXmlFK end) as RegXmlFK,t1.Status,t1.RegDt,t1.CnclDt,t1.CnfmNo,t1.Note,
(case when t1.ChrgdOrgFK = '-2147483648' 
THEN NULL else t1.ChrgdOrgFK end) as ChrgdOrgFK,
(case when t1.PayTermFK = '-2147483648' 
THEN NULL else t1.PayTermFK end) as PayTermFK,
(case when t1.ReimbInd = '-2147483648' 
THEN NULL else t1.ReimbInd end) as ReimbInd,t1.PaidDt,t1.BilledDt,
(CASE WHEN TrvlCost = '-9.99999999999999E14' 
THEN NULL ELSE TrvlCost END) AS TrvlCost,
(CASE WHEN OthrCost = '-9.99999999999999E14' 
THEN NULL ELSE OthrCost END) AS OthrCost,
(CASE WHEN ActualCost = '-9.99999999999999E14' 
THEN NULL ELSE ActualCost END) AS ActualCost,
(CASE WHEN ReimbAmt = '-9.99999999999999E14' 
THEN NULL ELSE ReimbAmt END) AS ReimbAmt,
(case when t1.CurrencyFK = '-2147483648' 
THEN NULL else t1.CurrencyFK end) as CurrencyFK,t1.WLNotifDt,t1.LstUpd,t1.SWLReserveExpiryDt,t1.UsrName,t1.Lck,t1.WLOverrideDt,Row_number() 
OVER(partition BY t1.regtrack_pk 
ORDER BY t1.LstUpd DESC) AS row_num 
FROM myLearning_1720.TBL_TMX_RegistrationTrack t1""")
InsZ_TMP_TBL_TMX_RegistrationTrack_Filtered_DF_Partitioned = InsZ_TMP_TBL_TMX_RegistrationTrack_Filtered_DF.repartition(InsZ_TMP_TBL_TMX_RegistrationTrack_Filtered_DF.row_num)
InsZ_TMP_TBL_TMX_RegistrationTrack_DF = InsZ_TMP_TBL_TMX_RegistrationTrack_Filtered_DF_Partitioned.filter(InsZ_TMP_TBL_TMX_RegistrationTrack_Filtered_DF.row_num==1)
InsZ_TMP_TBL_TMX_RegistrationTrack_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_TBL_TMX_RegistrationTrack")


#Default null handling for InsZ_INT_TBL_TMX_Registration and storing data to InsZ_INT_TBL_TMX_Registration
InsZ_INT_TBL_TMX_Registration_DF = spark.sql("""SELECT reg_pk,empfk,activityfk,status,maxattempts,allocationfk,cancellationdatetime,rmndnotedt,cnfmnotedt,cnclnotedt,certnotedt,extns,extclass,extid,validationkey,localpath,lstupd,usrname,lck,coursehours,estimatedtuition,estimatedtravelcost,estimatedhousingcost,cancelreasoncd,regexp_replace(cancelreasontext, '[\r\n]','') as cancelreasontext,cancelbillcd,cancelinitts,waivedreasoncd, regexp_replace(waivedreasontxt, '[\r\n]','') as waivedreasontxt,extintegtxntype,curriculumpriority,flighttype,wbs_fk,assetsordered,parttimefaculty,srtsource,srtcategory,srtlocationname,estimatedtuitionoverride,completedsurvey,main,docentid,certsponsor2id,certsponsor1id,interviewer_empfk,interview_schedule,cancellationrequested 
FROM myLearning_1720.InsZ_TMP_TBL_TMX_Registration_Filtered WHERE row_num=1""")
InsZ_INT_TBL_TMX_Registration_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_TBL_TMX_Registration")


#Default null handling for Inst and storing data to InsZ_INT_Inst
InsZ_TMP_Inst_Filtered_DF = spark.sql("""
SELECT inst_pk ,inst_venfk ,inst_no ,inst_ttl ,inst_lname ,inst_fname ,inst_title ,inst_mi ,inst_suffix ,inst_startdt ,inst_fullname ,inst_add1 ,inst_add2 ,inst_internalind ,inst_city ,inst_state ,inst_cntry ,inst_zip ,inst_email ,inst_phn1 ,inst_phn2 ,inst_fax ,inst_url ,inst_note ,inst_lstupd ,inst_usrname ,inst_lck ,inst_empfk ,inst_timestamp ,inst_timezonefk,Row_number() 
OVER(partition BY inst_pk 
ORDER BY inst_lstupd DESC) AS row_num 
FROM myLearning_1720.Inst""")
InsZ_TMP_Inst_Filtered_DF_Partitioned = InsZ_TMP_Inst_Filtered_DF.repartition(InsZ_TMP_Inst_Filtered_DF.row_num)
InsZ_TMP_Inst_DF = InsZ_TMP_Inst_Filtered_DF_Partitioned.filter(InsZ_TMP_Inst_Filtered_DF.row_num==1)
InsZ_TMP_Inst_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_Inst")

#Default null handling for iwc_Usr and storing data to InsZ_INT_iwc_Usr
InsZ_TMP_iwc_Usr_Filtered_DF = spark.sql("""
SELECT usr_pk ,usr_name ,usr_type ,usr_rolefk ,usr_password ,usr_enabled ,usr_empfk ,usr_ingeniumusrfk ,usr_ntlogin ,usr_desc ,usr_viewallempsind ,usr_lstupd ,usr_usrname ,usr_lck ,usr_pwdattempts ,usr_pwdchangedt ,usr_mustchangepwdind ,usr_currencyfk ,usr_timezonefk ,usr_userlangfk ,usr_extns ,usr_extclass ,usr_extid ,usr_isdeleted ,usr_curdomainfk ,usr_puid ,usr_allowws ,usr_emailformat ,usr_autopublishcalendar ,usr_uimode ,usr_hasusedwebextrial ,usr_userprefitemsperpage ,usr_name_old,Row_number() 
OVER(partition BY usr_pk 
ORDER BY usr_lstupd DESC) AS row_num 
FROM myLearning_1720.iwc_Usr""")
InsZ_TMP_iwc_Usr_Filtered_DF_Partitioned = InsZ_TMP_iwc_Usr_Filtered_DF.repartition(InsZ_TMP_iwc_Usr_Filtered_DF.row_num)
InsZ_TMP_iwc_Usr_DF = InsZ_TMP_iwc_Usr_Filtered_DF_Partitioned.filter(InsZ_TMP_iwc_Usr_Filtered_DF.row_num==1)
InsZ_TMP_iwc_Usr_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_iwc_Usr")

#Default null handling for TBL_TMX_RegTrack and storing data to InsZ_INT_TBL_TMX_RegTrack
InsZ_TMP_TBL_TMX_RegTrack_Filtered_DF = spark.sql("""
SELECT track_pk ,trackname ,trackdesc ,apprempfk ,activityfk ,type ,costbase ,costcnclbfdeadline ,costcnclafdeadline ,costnoshow ,currencyfk ,wltype ,swlexpirydt ,holdhrs ,openregdt ,approvalreqd ,costrule ,active ,pendingdeleteind ,lstupd ,usrname ,lck ,`timestamp`,Row_number() 
OVER(partition BY track_pk 
ORDER BY lstupd DESC) AS row_num 
FROM myLearning_1720.TBL_TMX_RegTrack""")
InsZ_TMP_TBL_TMX_RegTrack_Filtered_DF_Partitioned = InsZ_TMP_TBL_TMX_RegTrack_Filtered_DF.repartition(InsZ_TMP_TBL_TMX_RegTrack_Filtered_DF.row_num)
InsZ_TMP_TBL_TMX_RegTrack_DF = InsZ_TMP_TBL_TMX_RegTrack_Filtered_DF_Partitioned.filter(InsZ_TMP_TBL_TMX_RegTrack_Filtered_DF.row_num==1)
InsZ_TMP_TBL_TMX_RegTrack_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_TBL_TMX_RegTrack")

#Default null handling for MYL_TMX_RegLogistics and storing data to InsZ_INT_MYL_TMX_RegLogistics
InsZ_TMP_MYL_TMX_RegLogistics_Filtered_DF = spark.sql("""
SELECT reglogistics_reg_fk ,optfirstname,arvlairportcd,arvlairlinecd,arvlflightnum,airtranexpctdarvldtti,inbndgrndtransreqrdind,dprtairportcd,dprtairlinecd,dprtflightnum,airtranexpctddprtdtti,outbndgrndtransreqrdind,facilityarvldt,facilitydprtdt,hsgreqind,smoker,roomtype,uploadedtoextsys,reglogistics_lastupdatedby_emp_fk,lastupdatedts,otharvlairlinecode,othdeptairlinecode,grdtranconfirm,email,Row_number() 
OVER(partition BY reglogistics_reg_fk 
ORDER BY lastupdatedts DESC) AS row_num 
FROM myLearning_1720. MYL_TMX_RegLogistics""")
InsZ_TMP_MYL_TMX_RegLogistics_Filtered_DF_Partitoned = InsZ_TMP_MYL_TMX_RegLogistics_Filtered_DF.repartition(InsZ_TMP_MYL_TMX_RegLogistics_Filtered_DF.row_num)
InsZ_TMP_MYL_TMX_RegLogistics_DF = InsZ_TMP_MYL_TMX_RegLogistics_Filtered_DF_Partitoned.filter(InsZ_TMP_MYL_TMX_RegLogistics_Filtered_DF.row_num==1)
InsZ_TMP_MYL_TMX_RegLogistics_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_MYL_TMX_RegLogistics")

#Default null handling for TBL_TMX_ActLinks and storing data to InsZ_INT_TBL_TMX_ActLinks
InsZ_TMP_TBL_TMX_ActLinks_Filtered_DF = spark.sql("""
SELECT activityfk ,linkedactfk ,linktype ,rollupsklind ,rollupcompind,rolluprcrtind,lstupd ,usrname ,lck,Row_number() 
OVER(partition BY (activityfk,linkedactfk,linktype) 
ORDER BY lstupd DESC) AS row_num 
FROM myLearning_1720.TBL_TMX_ActLinks""")
InsZ_TMP_TBL_TMX_ActLinks_Filtered_DF_Partitioned = InsZ_TMP_TBL_TMX_ActLinks_Filtered_DF.repartition(InsZ_TMP_TBL_TMX_ActLinks_Filtered_DF.row_num)
InsZ_TMP_TBL_TMX_ActLinks_DF = InsZ_TMP_TBL_TMX_ActLinks_Filtered_DF_Partitioned.filter(InsZ_TMP_TBL_TMX_ActLinks_Filtered_DF.row_num==1)
InsZ_TMP_TBL_TMX_ActLinks_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_TBL_TMX_ActLinks")

#Default null handling for TBL_TMX_ActVen and storing data to InsZ_INT_TBL_TMX_ActVen
InsZ_TMP_TBL_TMX_ActVen_Filtered_DF = spark.sql("""
SELECT activityfk ,venfk ,lstupd ,usrname ,lck ,regexp_replace(vendordiscountinfo, '[\r\n]','') as vendordiscountinfo ,regexp_replace(vendorreginstructions, '[\r\n]','') as vendorreginstructions ,vendorcoursecode,Row_number() 
OVER(partition BY (activityfk,venfk) 
ORDER BY lstupd DESC) AS row_num 
FROM myLearning_1720.TBL_TMX_ActVen""")
InsZ_TMP_TBL_TMX_ActVen_Filtered_DF_Partitioned = InsZ_TMP_TBL_TMX_ActVen_Filtered_DF.repartition(InsZ_TMP_TBL_TMX_ActVen_Filtered_DF.row_num)
InsZ_TMP_TBL_TMX_ActVen_DF = InsZ_TMP_TBL_TMX_ActVen_Filtered_DF_Partitioned.filter(InsZ_TMP_TBL_TMX_ActVen_Filtered_DF.row_num==1)
InsZ_TMP_TBL_TMX_ActVen_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_TBL_TMX_ActVen")

#Default null handling for MYL_CurriculumCache and storing data to MYL_CurriculumCache
InsZ_INT_MYL_CurriculumCache_DF = spark.sql("""
SELECT t1.CurriculumCache_PK,t1.Emp_FK,t1.Activity_FK,t1.ActivityLabelFK,t1.RequiredLevel,t1.PreventVeto,
(case when t1.CurriculumID = '-2147483648' 
THEN NULL else t1.CurriculumID end) as CurriculumID,t1.curriculumDispCat 
FROM myLearning_1720.MYL_CurriculumCache t1""")
InsZ_INT_MYL_CurriculumCache_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.MYL_CurriculumCache")

#Default null handling for TBL_CPT_PlanningUnitBudget and storing data to InsZ_INT_TBL_CPT_PlanningUnitBudget
InsZ_INT_TBL_CPT_PlanningUnitBudget_DF = spark.sql("""
SELECT 
(CASE WHEN t1.PlanYearFK = '-2147483648' 
THEN NULL ELSE t1.PlanYearFK END) AS PlanYearFK,
(CASE WHEN t1.PlanningUnitFK = '-2147483648' 
THEN NULL ELSE t1.PlanningUnitFK END) AS PlanningUnitFK,
(CASE WHEN t1.Budget = '-9.99999999999999E14' 
THEN NULL ELSE t1.Budget END) AS Budget,
(CASE WHEN t1.BudgetHrs = '-9.99999999999999E14' 
THEN NULL ELSE t1.BudgetHrs END) AS BudgetHrs,t1.MDFactor,t1.BudgetGuideline,t1.LstUpdDt,t1.LstUptBy 
FROM mylearning_1720.TBL_CPT_PlanningUnitBudget t1""")
InsZ_INT_TBL_CPT_PlanningUnitBudget_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_TBL_CPT_PlanningUnitBudget")

#Default null handling for TBL_CPT_EmployeeFLAT and storing data to InsZ_INT_TBL_CPT_EmployeeFLAT
InsZ_INT_TBL_CPT_EmployeeFLAT_DF = spark.sql("""
SELECT EmpFK,
(CASE WHEN PeopleKey = '-2147483648' 
THEN NULL ELSE PeopleKey END) AS PeopleKey,PersonalNo,
(CASE WHEN CapabilityFK = '-2147483648' 
THEN NULL ELSE CapabilityFK END) AS CapabilityFK,
(CASE WHEN SpecialtyFK = '-2147483648' 
THEN NULL ELSE SpecialtyFK END) AS SpecialtyFK,
(CASE WHEN GeographicFK = '-2147483648' 
THEN NULL ELSE GeographicFK END) AS GeographicFK,
(CASE WHEN CountryFK = '-2147483648' 
THEN NULL ELSE CountryFK END) AS CountryFK,
(CASE WHEN CareerLevelFK = '-2147483648' 
THEN NULL ELSE CareerLevelFK END) AS CareerLevelFK,
(CASE WHEN CareerLevelGroupFK = '-2147483648' 
THEN NULL ELSE CareerLevelGroupFK END) AS CareerLevelGroupFK,
(CASE WHEN isMD = '-2147483648' 
THEN NULL ELSE isMD END) AS isMD,
(CASE WHEN RoleFK = '-2147483648' 
THEN NULL ELSE RoleFK END) AS RoleFK,
(CASE WHEN IndustryFK = '-2147483648' 
THEN NULL ELSE IndustryFK END) AS IndustryFK,
(CASE WHEN IndustryDtlFK = '-2147483648' 
THEN NULL ELSE IndustryDtlFK END) AS IndustryDtlFK,
(CASE WHEN TalentSegmentFK = '-2147483648' 
THEN NULL ELSE TalentSegmentFK END) AS TalentSegmentFK,
(CASE WHEN CareerTrackFK = '-2147483648' 
THEN NULL ELSE CareerTrackFK END) AS CareerTrackFK,
(CASE WHEN OrgUnitID = '-2147483648' 
THEN NULL ELSE OrgUnitID END) AS OrgUnitID,
(CASE WHEN CostCenterFK = '-2147483648' 
THEN NULL ELSE CostCenterFK END) AS CostCenterFK,
(CASE WHEN PlanningUnitFK = '-2147483648' 
THEN NULL ELSE PlanningUnitFK END) AS PlanningUnitFK,
(CASE WHEN PlanningUnitGroupFK = '-2147483648' 
THEN NULL ELSE PlanningUnitGroupFK END) AS PlanningUnitGroupFK,
(CASE WHEN DTEFK = '-2147483648' 
THEN NULL ELSE DTEFK END) AS DTEFK,
(CASE WHEN PerCapitaBudget = '-9.99999999999999E14' 
THEN NULL ELSE PerCapitaBudget END) AS PerCapitaBudget,
(CASE WHEN BudgetHours = '-9.99999999999999E14' 
THEN NULL ELSE BudgetHours END) AS BudgetHours,
(CASE WHEN RoleFamilyFK = '-2147483648' 
THEN NULL ELSE RoleFamilyFK END) AS RoleFamilyFK,
(CASE WHEN IsFromLive = '-2147483648' 
THEN NULL ELSE IsFromLive END) AS IsFromLive,
(CASE WHEN SubRegionFK = '-2147483648' 
THEN NULL ELSE SubRegionFK END) AS SubRegionFK 
FROM mylearning_1720.TBL_CPT_EmployeeFLAT""")
InsZ_INT_TBL_CPT_EmployeeFLAT_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_TBL_CPT_EmployeeFLAT")

#Default null handling for TBL_CPT_CourseEmployeeCommonality_UPDT and storing data to InsZ_INT_TBL_CPT_CourseEmployeeCommonality_UPDT
InsZ_INT_TBL_CPT_CourseEmployeeCommonality_UPDT_DF = spark.sql("""
SELECT ActivityFK,
(CASE WHEN UserFK = '-2147483648' 
THEN NULL ELSE UserFK END) AS UserFK,
(CASE WHEN PlanningUnitFK = '-2147483648' 
THEN NULL ELSE PlanningUnitFK END) AS PlanningUnitFK,
(CASE WHEN PlanningUnitGroupFK = '-2147483648' 
THEN NULL ELSE PlanningUnitGroupFK END) AS PlanningUnitGroupFK,
(CASE WHEN DTEFK = '-2147483648' 
THEN NULL ELSE DTEFK END) AS DTEFK,
(CASE WHEN CountTowardsIncurred = '-2147483648' 
THEN NULL ELSE CountTowardsIncurred END) AS CountTowardsIncurred,
(CASE WHEN CountTowardsCommitted = '-2147483648' 
THEN NULL ELSE CountTowardsCommitted END) AS CountTowardsCommitted,Status,CancelBillCd,
(CASE WHEN CancelCost = '-9.99999999999999E14' 
THEN NULL ELSE CancelCost END) AS CancelCost,
(CASE WHEN CourseHours = '-9.99999999999999E14' 
THEN NULL ELSE CourseHours END) AS CourseHours,
(CASE WHEN EstimatedTuition = '-9.99999999999999E14' 
THEN NULL ELSE EstimatedTuition END) AS EstimatedTuition,
(CASE WHEN EstimatedHousingCost = '-9.99999999999999E14' 
THEN NULL ELSE EstimatedHousingCost END) AS EstimatedHousingCost,
(CASE WHEN EstimatedTravelCost = '-9.99999999999999E14' 
THEN NULL ELSE EstimatedTravelCost END) AS EstimatedTravelCost 
FROM mylearning_1720.TBL_CPT_CourseEmployeeCommonality_UPDT """)
InsZ_INT_TBL_CPT_CourseEmployeeCommonality_UPDT_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_TBL_CPT_CourseEmployeeCommonality_UPDT")

#Default null handling for MYL_CG_StatusCache and storing data to InsZ_INT_MYL_CG_StatusCache
InsZ_INT_MYL_CG_StatusCache_DF = spark.sql("""
SELECT 
(CASE WHEN EmpFK = '-2147483648' 
THEN NULL ELSE EmpFK END) AS EmpFK,
(CASE WHEN GrpActID = '-2147483648' 
THEN NULL ELSE GrpActID END) AS GrpActID,
(CASE WHEN CrsActID = '-2147483648' 
THEN NULL ELSE CrsActID END) AS CrsActID,
(CASE WHEN SnActID = '-2147483648' 
THEN NULL ELSE SnActID END) AS SnActID,Status,
(CASE WHEN RegPK = '-2147483648' 
THEN NULL ELSE RegPK END) AS RegPK,
(CASE WHEN AttemptPK = '-2147483648' 
THEN NULL ELSE AttemptPK END) AS AttemptPK,ActionList,SnName,SnStartDt,SnEndDt,FacName,FacCity,FacState,FacURL,WBSNumber,
(CASE WHEN TimezoneID = '-2147483648' 
THEN NULL ELSE TimezoneID END) AS TimezoneID,FacAdd1,FacAdd2,FacZip,Faccountryname,Notes,
(CASE WHEN TuitionGracePeriod = '-2147483648' 
THEN NULL ELSE TuitionGracePeriod END) AS TuitionGracePeriod,
(CASE WHEN HousingGracePeriod = '-2147483648' 
THEN NULL ELSE HousingGracePeriod END) AS HousingGracePeriod,
(CASE WHEN RefreshFlag = '-2147483648' 
THEN NULL ELSE RefreshFlag END) AS RefreshFlag 
FROM mylearning_1720.MYL_CG_StatusCache""")
InsZ_INT_MYL_CG_StatusCache_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_MYL_CG_StatusCache")

#Default null handling for Myl_ReqCurrDueDate and storing data to InsZ_INT_Myl_ReqCurrDueDate
InsZ_INT_Myl_ReqCurrDueDate_DF = spark.sql("""
SELECT 
(CASE WHEN EmpFK = '-2147483648' 
THEN NULL ELSE EmpFK END) AS EmpFK,
(CASE WHEN Curr_Id = '-2147483648' 
THEN NULL ELSE Curr_Id END) AS Curr_Id,
(CASE WHEN AudienceFk = '-2147483648' 
THEN NULL ELSE AudienceFk END) AS AudienceFk,EntryDate,DueDate,
(CASE WHEN IsActive = '-2147483648' 
THEN NULL ELSE IsActive END) AS IsActive,DepartureDate,
(CASE WHEN ModifiedJob = '-2147483648' 
THEN NULL ELSE ModifiedJob END) AS ModifiedJob,
(CASE WHEN RecalculateDate = '-2147483648' 
THEN NULL ELSE RecalculateDate END) AS RecalculateDate 
FROM mylearning_1720.Myl_ReqCurrDueDate""")
InsZ_INT_Myl_ReqCurrDueDate_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_Myl_ReqCurrDueDate")

#Default null handling for TBL_TMX_ActAudReq and storing data to InsZ_INT_TBL_TMX_ActAudReq
InsZ_INT_TBL_TMX_ActAudReq_DF = spark.sql("""
SELECT ActivityFK,AudienceFK,
(CASE WHEN PrtyFK = '-2147483648' 
THEN NULL ELSE PrtyFK END) AS PrtyFK,ReqInd,
(CASE WHEN DueRule = '-2147483648' 
THEN NULL ELSE DueRule END) AS DueRule,DueDate,
(CASE WHEN DueDays = '-2147483648' 
THEN NULL ELSE DueDays END) AS DueDays,PlanDate,LstUpd,UsrName,Lck,dueText 
FROM mylearning_1720.TBL_TMX_ActAudReq""")
InsZ_INT_TBL_TMX_ActAudReq_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_TBL_TMX_ActAudReq")

#Default null handling for TBL_TMX_Audience and storing data to InsZ_INT_TBL_TMX_Audience
InsZ_INT_TBL_TMX_Audience_DF = spark.sql("""
SELECT Aud_PK,Aud_Name,Aud_Desc,Aud_ActiveInd,
(CASE WHEN Aud_EmpFK = '-2147483648' 
THEN NULL ELSE Aud_EmpFK END) AS Aud_EmpFK,Aud_LstUpd,Aud_UsrName,Aud_Lck,Aud_curriculumFlag,
(CASE WHEN Aud_docentID = '-2147483648' 
THEN NULL ELSE Aud_docentID END) AS Aud_docentID,UploadFlag,BatchEnrollFlag,IsMyLX,
(CASE WHEN isCPTAudience = '-2147483648' 
THEN NULL ELSE isCPTAudience END) AS isCPTAudience 
FROM mylearning_1720.TBL_TMX_Audience""")
InsZ_INT_TBL_TMX_Audience_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_TBL_TMX_Audience")

#Default null handling for TBL_CPT_CourseEmployeeCommonality and storing data to InsZ_INT_TBL_CPT_CourseEmployeeCommonality
InsZ_TMP_TBL_CPT_CourseEmployeeCommonality_Filtered_DF = spark.sql("""
SELECT CourseId,UserFK,
(CASE WHEN AudFK = '-2147483648' 
THEN NULL ELSE AudFK END) AS AudFK,PlanningUnitFK,PlanningUnitGroupFK,
(CASE WHEN IsRequired = '-2147483648' 
THEN NULL ELSE IsRequired END) AS IsRequired,
(CASE WHEN LocalCost = '-9.99999999999999E14' 
THEN NULL ELSE LocalCost END) AS LocalCost,
(CASE WHEN CentralCost = '-9.99999999999999E14' 
THEN NULL ELSE CentralCost END) AS CentralCost,
(CASE WHEN Hours = '-9.99999999999999E14' 
THEN NULL ELSE Hours END) AS Hours,DTEFK,
(CASE WHEN ExpectedEnrollment = '-9.99999999999999E14' 
THEN NULL ELSE ExpectedEnrollment END) AS ExpectedEnrollment,
(CASE WHEN priorCompleted = '-2147483648' 
THEN NULL ELSE priorCompleted END) AS priorCompleted,
(CASE WHEN CurriculumID = '-2147483648' 
THEN NULL ELSE CurriculumID END) AS CurriculumID,
(CASE WHEN PlannedEnrollment = '-9.99999999999999E14' 
THEN NULL ELSE PlannedEnrollment END) AS PlannedEnrollment,
(CASE WHEN isWaiver = '-2147483648' 
THEN NULL ELSE isWaiver END) AS isWaiver,
(CASE WHEN CountTowardsIncurred = '-2147483648' 
THEN NULL ELSE CountTowardsIncurred END) AS CountTowardsIncurred,
(CASE WHEN CountTowardsCommitted = '-2147483648' 
THEN NULL ELSE CountTowardsCommitted END) AS CountTowardsCommitted,LocalOrCentral,Status,CancelBillCd,
(CASE WHEN CancelCost = '-9.99999999999999E14' 
THEN NULL ELSE CancelCost END) AS CancelCost,Row_number() 
OVER(partition BY (CourseId,UserFK) 
ORDER BY audfk desc ) AS row_num  
FROM mylearning_1720.TBL_CPT_CourseEmployeeCommonality""")
InsZ_TMP_TBL_CPT_CourseEmployeeCommonality_Filtered_DF_Partitioned = InsZ_TMP_TBL_CPT_CourseEmployeeCommonality_Filtered_DF.repartition(InsZ_TMP_TBL_CPT_CourseEmployeeCommonality_Filtered_DF.row_num)
InsZ_TMP_TBL_CPT_CourseEmployeeCommonality_DF = InsZ_TMP_TBL_CPT_CourseEmployeeCommonality_Filtered_DF_Partitioned.filter(InsZ_TMP_TBL_CPT_CourseEmployeeCommonality_Filtered_DF.row_num==1)
InsZ_TMP_TBL_CPT_CourseEmployeeCommonality_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_TBL_CPT_CourseEmployeeCommonality")

#Default null handling for CPT_MFP_CostCenter_Table and storing data to InsZ_INT_CPT_MFP_CostCenter_Table
InsZ_INT_CPT_MFP_CostCenter_Table_DF = spark.sql("""
SELECT geographicarea,geographicunitcode,geographicunit,countrycode,country,cclevel1,cclevel1desc,cclevel2,cclevel2desc,cclevel3,cclevel3desc,cclevel4,cclevel4desc,cclevel5,cclevel5desc,careertrack,costcentercode,costcenter,profitcentercode,profitcenter,typeofwork,
(CASE WHEN dtefk = '-2147483648' 
THEN NULL ELSE dtefk END) AS dtefk,
(CASE WHEN pugfk = '-2147483648' 
THEN NULL ELSE pugfk END) AS pugfk,
(CASE WHEN pufk = '-2147483648' 
THEN NULL ELSE pufk END) AS pufk,
(CASE WHEN country_orgfk = '-2147483648' 
THEN NULL ELSE country_orgfk END) AS country_orgfk,
(CASE WHEN careertrack_orgfk = '-2147483648' 
THEN NULL ELSE careertrack_orgfk END) AS careertrack_orgfk 
FROM myLearning_1720.CPT_MFP_CostCenter_Table""")
InsZ_INT_CPT_MFP_CostCenter_Table_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_CPT_MFP_CostCenter_Table")

#Default null handling for TBL_CPT_BusinessDTE and storing data to InsZ_INT_TBL_CPT_BusinessDTE
InsZ_INT_TBL_CPT_BusinessDTE_DF = spark.sql("""
SELECT 
(CASE WHEN dte_pk = '-2147483648' 
THEN NULL ELSE dte_pk END) AS dte_pk,code,name,lstupddt,lstuptby,
(CASE WHEN globaltpfactor = '-9.99999999999999E14' 
THEN NULL ELSE globaltpfactor END) AS globaltpfactor,
(CASE WHEN planyearfk = '-2147483648' 
THEN NULL ELSE planyearfk END) AS planyearfk 
FROM myLearning_1720.TBL_CPT_BusinessDTE""")
InsZ_INT_TBL_CPT_BusinessDTE_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_TBL_CPT_BusinessDTE")

#Default null handling for TBL_CPT_Course and storing data to InsZ_INT_TBL_CPT_Course
InsZ_INT_TBL_CPT_Course_DF = spark.sql("""
SELECT courseid,activityfk,planyearfk,
(CASE WHEN hours = '-9.99999999999999E14' 
THEN NULL ELSE hours END) AS hours,
(CASE WHEN centraltution = '-9.99999999999999E14' 
THEN NULL ELSE centraltution END) AS centraltution,
(CASE WHEN centralhousing = '-9.99999999999999E14' 
THEN NULL ELSE centralhousing END) AS centralhousing,
(CASE WHEN centralhousingdays = '-2147483648' 
THEN NULL ELSE centralhousingdays END) AS centralhousingdays,
(CASE WHEN centraltravel = '-9.99999999999999E14' 
THEN NULL ELSE centraltravel END) AS centraltravel,commonalityrefreshneeded,
(CASE WHEN isrequired = '-2147483648' 
THEN NULL ELSE isrequired END) AS isrequired,
(CASE WHEN iseditableonpcl = '-2147483648' 
THEN NULL ELSE iseditableonpcl END) AS iseditableonpcl,
(CASE WHEN ownerempfk = '-2147483648' 
THEN NULL ELSE ownerempfk END) AS ownerempfk,
(CASE WHEN activitylabelfk = '-2147483648' 
THEN NULL ELSE activitylabelfk END) AS activitylabelfk,lemtd_name,code,activityname,courseclassification 
FROM myLearning_1720.TBL_CPT_Course""")
InsZ_INT_TBL_CPT_Course_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_TBL_CPT_Course")

#Default null handling for TBL_CPT_PlanningUnitGroup and storing data to InsZ_INT_TBL_CPT_PlanningUnitGroup
InsZ_INT_TBL_CPT_PlanningUnitGroup_DF = spark.sql("""
SELECT planningunitgroup_pk,code,name,
(CASE WHEN dtefk = '-2147483648' 
THEN NULL ELSE dtefk END) AS dtefk,lstupddt,lstuptby,
(CASE WHEN planyearfk = '-2147483648' 
THEN NULL ELSE planyearfk END) AS planyearfk 
FROM myLearning_1720.TBL_CPT_PlanningUnitGroup""")
InsZ_INT_TBL_CPT_PlanningUnitGroup_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_TBL_CPT_PlanningUnitGroup")

#Default null handling for TBL_CPT_PlanningUnit and storing data to InsZ_INT_TBL_CPT_PlanningUnit
InsZ_INT_TBL_CPT_PlanningUnit_DF = spark.sql("""
SELECT planningunit_pk,code,name,
(CASE WHEN planningunitgroupfk = '-2147483648' 
THEN NULL ELSE planningunitgroupfk END) AS planningunitgroupfk,costcenternode,lstupddt,lstuptby,
(CASE WHEN planyearfk = '-2147483648' 
THEN NULL ELSE planyearfk END) AS planyearfk 
FROM myLearning_1720.TBL_CPT_PlanningUnit""")
InsZ_INT_TBL_CPT_PlanningUnit_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_TBL_CPT_PlanningUnit")

#Default null handling for TBL_CPT_PlanningUnit_CostCenterMapping and storing data to InsZ_INT_TBL_CPT_PlanningUnit_CostCenterMapping
InsZ_INT_TBL_CPT_PlanningUnit_CostCenterMapping_DF = spark.sql("""
SELECT 
(CASE WHEN planningunitfk = '-2147483648' 
THEN NULL ELSE planningunitfk END) AS planningunitfk,
(CASE WHEN costcenterfk = '-2147483648' 
THEN NULL ELSE costcenterfk END) AS costcenterfk,lstupddt,lstuptby 
FROM myLearning_1720.TBL_CPT_PlanningUnit_CostCenterMapping""")
InsZ_INT_TBL_CPT_PlanningUnit_CostCenterMapping_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_TBL_CPT_PlanningUnit_CostCenterMapping")

#Default null handling for TBL_CPT_CoursePUMapping and storing data to InsZ_INT_TBL_CPT_CoursePUMapping
InsZ_INT_TBL_CPT_CoursePUMapping_DF = spark.sql("""
SELECT 
(CASE WHEN courseid = '-2147483648' 
THEN NULL ELSE courseid END) AS courseid,
(CASE WHEN planningunitfk = '-2147483648' 
THEN NULL ELSE planningunitfk END) AS planningunitfk,
(CASE WHEN localtravel = '-9.99999999999999E14' 
THEN NULL ELSE localtravel END) AS localtravel,
(CASE WHEN localhousing = '-9.99999999999999E14' 
THEN NULL ELSE localhousing END) AS localhousing,
(CASE WHEN localtuition = '-9.99999999999999E14' 
THEN NULL ELSE localtuition END) AS localtuition,
(CASE WHEN localhousingdays = '-9.99999999999999E14' 
THEN NULL ELSE localhousingdays END) AS localhousingdays,
(CASE WHEN percentlocal = '-9.99999999999999E14' 
THEN NULL ELSE percentlocal END) AS percentlocal,
(CASE WHEN centraltravelcost = '-9.99999999999999E14' 
THEN NULL ELSE centraltravelcost END) AS centraltravelcost,mappingid,
(CASE WHEN noncompletes_snapshot1 = '-2147483648' 
THEN NULL ELSE noncompletes_snapshot1 END) AS noncompletes_snapshot1,
(CASE WHEN expectedenrollment_snapshot1 = '-2147483648' 
THEN NULL ELSE expectedenrollment_snapshot1 END) AS expectedenrollment_snapshot1,
(CASE WHEN expectedtotalhrs_snapshot1 = '-9.99999999999999E14' 
THEN NULL ELSE expectedtotalhrs_snapshot1 END) AS expectedtotalhrs_snapshot1,
(CASE WHEN expectedtotalcost_snapshot1 = '-9.99999999999999E14' 
THEN NULL ELSE expectedtotalcost_snapshot1 END) AS expectedtotalcost_snapshot1,
(CASE WHEN noncompletes_snapshot2 = '-2147483648' 
THEN NULL ELSE noncompletes_snapshot2 END) AS noncompletes_snapshot2,
(CASE WHEN expectedenrollment_snapshot2 = '-2147483648' 
THEN NULL ELSE expectedenrollment_snapshot2 END) AS expectedenrollment_snapshot2,
(CASE WHEN expectedtotalhrs_snapshot2 = '-9.99999999999999E14' 
THEN NULL ELSE expectedtotalhrs_snapshot2 END) AS expectedtotalhrs_snapshot2,
(CASE WHEN expectedtotalcost_snapshot2 = '-9.99999999999999E14' 
THEN NULL ELSE expectedtotalcost_snapshot2 END) AS expectedtotalcost_snapshot2,regionaltrainingcenter,
(CASE WHEN plannedenrollment = '-9.99999999999999E14' 
THEN NULL ELSE plannedenrollment END) AS plannedenrollment,
(CASE WHEN prioryearcompletion = '-2147483648' 
THEN NULL ELSE prioryearcompletion END) AS prioryearcompletion 
FROM myLearning_1720.TBL_CPT_CoursePUMapping""")
InsZ_INT_TBL_CPT_CoursePUMapping_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_TBL_CPT_CoursePUMapping")

#Default null handling for TBL_CPT_Curriculum and storing data to InsZ_INT_TBL_CPT_Curriculum
InsZ_INT_TBL_CPT_Curriculum_DF = spark.sql("""
SELECT curriculumid,curractivityfk,capsolsheetname,
(CASE WHEN isrequired = '-2147483648' 
THEN NULL ELSE isrequired END) AS isrequired,currreportingcategory,crsexecutivesponsor,planyearfk,donotpublish 
FROM myLearning_1720.TBL_CPT_Curriculum""")
InsZ_INT_TBL_CPT_Curriculum_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_TBL_CPT_Curriculum")

#Default null handling for MYL_CostCenter and storing data to InsZ_INT_MYL_CostCenter
InsZ_INT_MYL_CostCenter_DF = spark.sql("""
SELECT costcenter_pk ,costcenter_name ,costcenter_code ,
(CASE WHEN costcenter_operatinggroup_fk = '-2147483648' 
THEN NULL ELSE costcenter_operatinggroup_fk END) AS costcenter_operatinggroup_fk,costcenter_companycode ,costcenter_status 
FROM mylearning_1720.MYL_CostCenter""")
InsZ_INT_MYL_CostCenter_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_MYL_CostCenter")

#Default null handling for TBL_CPT_CourseCurriculumMapping and storing data to Insz_Int_TBL_CPT_CourseCurriculumMapping
Insz_Int_TBL_CPT_CourseCurriculumMapping_DF = spark.sql("""
SELECT crscurr_pk,courseid,curriculumid 
FROM mylearning_1720.TBL_CPT_CourseCurriculumMapping""")
Insz_Int_TBL_CPT_CourseCurriculumMapping_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.Insz_Int_TBL_CPT_CourseCurriculumMapping")

#Default null handling for TBL_CPT_CurrAudMapping and storing data to Insz_Int_TBL_CPT_CurrAudMapping
Insz_Int_TBL_CPT_CurrAudMapping_DF = spark.sql("""
SELECT curraud_pk,curriculumid,audiencefk 
FROM mylearning_1720.TBL_CPT_CurrAudMapping""")
Insz_Int_TBL_CPT_CurrAudMapping_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.Insz_Int_TBL_CPT_CurrAudMapping")

#Default null handling for Myl_AccentureLocation and storing data to InsZ_INT_Myl_AccentureLocation
InsZ_INT_Myl_AccentureLocation_DF = spark.sql("""
SELECT accentureloc_pk,accentureloc_name,accentureloc_shortname,accentureloc_code,accentureloc_city,accentureloc_state,accentureloc_zip,accentureloc_metrocity_fk,accentureloc_status 
FROM mylearning_1720.Myl_AccentureLocation""")
InsZ_INT_Myl_AccentureLocation_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_Myl_AccentureLocation")

#Default null handling for MYL_DTEProfitCenter and storing data to InsZ_INT_MYL_DTEProfitCenter
InsZ_INT_MYL_DTEProfitCenter_DF = spark.sql("""
SELECT dteprofitcenter_dte_fk,dteprofitcenter_pc_fk,recalcytdactualamts 
FROM mylearning_1720.MYL_DTEProfitCenter""")
InsZ_INT_MYL_DTEProfitCenter_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_MYL_DTEProfitCenter")

#Default null handling for EmpStat and storing data to InsZ_INT_EmpStat
InsZ_INT_EmpStat_DF = spark.sql("""
SELECT empstatpk,empstatname,empstatlstupd,empstatusrname,empstatlck 
FROM mylearning_1720.EmpStat""")
InsZ_INT_EmpStat_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_EmpStat")

#Default null handling for LEMTD and storing data to InsZ_INT_LEMTD
InsZ_INT_LEMTD_DF = spark.sql("""
SELECT lemtdpk,lemtdname,lemtdlstupd,lemtdusrname,lemtdlck,lemtdtimestamp,lemtdiconname 
FROM mylearning_1720.LEMTD""")
InsZ_INT_LEMTD_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_LEMTD")

#Default null handling for actLabel and storing data to InsZ_INT_actLabel
InsZ_INT_actLabel_DF = spark.sql("""
SELECT actlabelpk,actlabelname,acttemplatefk,offerstypefk,labeldesc,icon,lstupd,usrname,lck,`timestamp`,searchresulttype 
FROM mylearning_1720.actLabel""")
InsZ_INT_actLabel_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_actLabel")

#Default null handling for tbl_tmx_RuleSetAud and storing data to InsZ_INT_tbl_tmx_RuleSetAud
InsZ_INT_tbl_tmx_RuleSetAud_DF = spark.sql("""
SELECT rulesetaudience_audiencefk,rulesetaudience_rulesetfk,rulesetaudience_lstupd,rulesetaudience_usrname,rulesetaudience_lck 
FROM mylearning_1720.tbl_tmx_RuleSetAud""")
InsZ_INT_tbl_tmx_RuleSetAud_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_tbl_tmx_RuleSetAud")

#Default null handling for MYL_ObjectOwner and storing data to InsZ_INT_MYL_ObjectOwner
InsZ_INT_MYL_ObjectOwner_DF = spark.sql("""
SELECT objectownerpk,objectidfk,owneridfk,objecttype 
FROM mylearning_1720.MYL_ObjectOwner""")
InsZ_INT_MYL_ObjectOwner_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_MYL_ObjectOwner")

#Default null handling for tbl_tmx_rulesetdataset and storing data to InsZ_INT_tbl_tmx_rulesetdataset
InsZ_INT_tbl_tmx_rulesetdataset_DF = spark.sql("""
SELECT ruleset_rulesetfk,ruleset_datasetfk 
FROM mylearning_1720.tbl_tmx_rulesetdataset""")
InsZ_INT_tbl_tmx_rulesetdataset_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_tbl_tmx_rulesetdataset")

#Default null handling for vw_tmx_datasetusers and storing data to InsZ_INT_vw_tmx_datasetusers
InsZ_INT_vw_tmx_datasetusers_DF = spark.sql("""
SELECT datasetusers_datasetfk,datasetusers_empfk 
FROM mylearning_1720.vw_tmx_datasetusers""")
InsZ_INT_vw_tmx_datasetusers_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_vw_tmx_datasetusers")

#Default null handling for MYL_Code_Decode_Category and storing data to InsZ_INT_MYL_Code_Decode_Category
InsZ_INT_MYL_Code_Decode_Category_DF = spark.sql("""
SELECT cdtid,cdtname,cdtlstupd,cdtusrname,cdtlck 
FROM mylearning_1720.MYL_Code_Decode_Category""")
InsZ_INT_MYL_Code_Decode_Category_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_MYL_Code_Decode_Category")

#Default null handling for MYL_TMX_RegLogistics and storing data to Insz_Int_MYL_TMX_RegLogistics
Insz_Int_MYL_TMX_RegLogistics_DF = spark.sql("""
SELECT reglogistics_reg_fk,optfirstname,arvlairportcd,arvlairlinecd,arvlflightnum,airtranexpctdarvldtti,inbndgrndtransreqrdind,dprtairportcd,dprtairlinecd,dprtflightnum,airtranexpctddprtdtti,outbndgrndtransreqrdind,facilityarvldt,facilitydprtdt,hsgreqind,smoker,roomtype,uploadedtoextsys,reglogistics_lastupdatedby_emp_fk,lastupdatedts,otharvlairlinecode,othdeptairlinecode,grdtranconfirm,email 
FROM mylearning_1720.MYL_TMX_RegLogistics""")
Insz_Int_MYL_TMX_RegLogistics_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.Insz_Int_MYL_TMX_RegLogistics")

#Default null handling for EmpCd and storing data to Insz_Int_EmpCd
Insz_Int_EmpCd_DF = spark.sql("""
SELECT empcdpk,empcdname,empcdlstupd,empcdusrname,empcdlck,acnempcdempnonemp 
FROM mylearning_1720.EmpCd""")
Insz_Int_EmpCd_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.Insz_Int_EmpCd")

#Default null handling for iwc_Role and storing data to Insz_Int_iwc_Role
Insz_Int_iwc_Role_DF = spark.sql("""
SELECT rolepk,rolename,roledesc,rolelstupd,roleusrname,rolelck,updatetimestamp,roledomainfk,roledefaultind,roleinheritind,roleactiveind,rolebaserolefk,rolerolemask,rolerank,rolerestricted 
FROM mylearning_1720.iwc_Role""")
Insz_Int_iwc_Role_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.Insz_Int_iwc_Role")

#Default null handling for iwc_RolePerm and storing data to Insz_Int_iwc_RolePerm
Insz_Int_iwc_RolePerm_DF = spark.sql("""
SELECT roleperm_rolefk,roleperm_name,roleperm_value 
FROM mylearning_1720.iwc_RolePerm""")
Insz_Int_iwc_RolePerm_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.Insz_Int_iwc_RolePerm")

#Default null handling for MYL_CentraMeetingRequest and storing data to Insz_Int_MYL_CentraMeetingRequest
Insz_Int_MYL_CentraMeetingRequest_DF = spark.sql("""
SELECT cmr_pk,session_activity_fk,cmr_status,cmr_eventtype,cmr_participants,cmr_breakoutrooms,cmr_appshare,cmr_trainingtype,cmr_deliverysupport,cmr_audio,cmr_recording,cmr_chargenum,cmr_srdrequested,cmr_deliverycontact_emp_fk,cmr_moderatorcontact_emp_fk,cmr_onairsupportcontact_emp_fk,cmr_mediacontact_emp_fk,cmr_participantannouncement,cmr_participantlogistics,cmr_facultycommunication,cmr_materialuploaded,cmr_facultyprep,cmr_wrapupcompleted,cmr_solutionplanner_emp_fk,cmr_selfservice,cmr_comments,cmr_requestor,cmr_onairsupportcontact2_emp_fk 
FROM mylearning_1720.MYL_CentraMeetingRequest""")
Insz_Int_MYL_CentraMeetingRequest_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.Insz_Int_MYL_CentraMeetingRequest")

#Default null handling for Myl_PHGroupingCategory and storing data to Insz_Int_Myl_PHGroupingCategory
Insz_Int_Myl_PHGroupingCategory_DF = spark.sql("""
SELECT phgroupingcategory_pk,phgroupingcategoryid,phgroupingcategory,status 
FROM mylearning_1720.Myl_PHGroupingCategory""")
Insz_Int_Myl_PHGroupingCategory_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.Insz_Int_Myl_PHGroupingCategory")

#Default null handling for Skl and storing data to Insz_Int_Skl
Insz_Int_Skl_DF = spark.sql("""
SELECT sklpk,sklcontypefk,sklname,skldesc,sklurl,skllstupd,sklusrname,skllck,sklprofscalefk,sklqueststem,skltimestamp,sklcd,sklabbr,sklstatus,skltypefk,sklcategoryfk,sklgroupfk 
FROM mylearning_1720.Skl""")
Insz_Int_Skl_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.Insz_Int_Skl")

#Default null handling for TBL_TAX_ConType and storing data to Insz_Int_TBL_TAX_ConType
Insz_Int_TBL_TAX_ConType_DF = spark.sql("""
SELECT contypeidpk,contypename,icon,ordinal,lstupd,username 
FROM mylearning_1720.TBL_TAX_ConType""")
Insz_Int_TBL_TAX_ConType_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.Insz_Int_TBL_TAX_ConType")

#Default null handling for TBL_TAX_FileType and storing data to Insz_Int_TBL_TAX_FileType
Insz_Int_TBL_TAX_FileType_DF = spark.sql("""
SELECT filetypeidpk,filetypename,icon,mimetype,extensions,safety,lstupd,username 
FROM mylearning_1720.TBL_TAX_FileType""")
Insz_Int_TBL_TAX_FileType_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.Insz_Int_TBL_TAX_FileType")

#Default null handling for TBL_TAX_NodeRel and storing data to Insz_Int_TBL_TAX_NodeRel
Insz_Int_TBL_TAX_NodeRel_DF = spark.sql("""
SELECT prntnodeid_fk,nodeid_fk 
FROM mylearning_1720.TBL_TAX_NodeRel""")
Insz_Int_TBL_TAX_NodeRel_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.Insz_Int_TBL_TAX_NodeRel")

#Default null handling for TBL_TAX_State and storing data to Insz_Int_TBL_TAX_State
Insz_Int_TBL_TAX_State_DF = spark.sql("""
SELECT stateidpk,statename,lstupd,username 
FROM mylearning_1720.TBL_TAX_State""")
Insz_Int_TBL_TAX_State_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.Insz_Int_TBL_TAX_State")

#Default null handling for TBL_TMX_ActRel and storing data to Insz_Int_TBL_TMX_ActRel
Insz_Int_TBL_TMX_ActRel_DF = spark.sql("""
SELECT activityfk,parentactivityfk 
FROM mylearning_1720.TBL_TMX_ActRel""")
Insz_Int_TBL_TMX_ActRel_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.Insz_Int_TBL_TMX_ActRel")

#Default null handling for tbl_businessactivities and storing data to InsZ_INT_tbl_businessactivities
InsZ_INT_tbl_businessactivities_DF = spark.sql("""
SELECT businessactivitylev1, businessactivitylev1descr, businessactivitylev2, businessactivitylev2descr, businessactivitylev3, businessactivitylev3descr, rowstatusfl 
FROM mrdr_1033.tbl_businessactivities""")
InsZ_INT_tbl_businessactivities_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_tbl_businessactivities")

#Default null handling for tbl_CostElement and storing data to InsZ_INT_tbl_CostElement
InsZ_INT_tbl_CostElement_DF = spark.sql("""
SELECT chartofaccounts, costelement, createdon, enteredby ,
(CASE WHEN TRIM(TaxRelevantFl) = '' 
THEN NULL ELSE TaxRelevantFl END) AS taxrelevantfl ,affectingpaymentfl ,blank_1 ,functionalarea ,validtodt ,validfromdt ,category ,blank_2 ,planningaccessfl ,planninglocationfl ,planninguserfl ,blank_3 ,unitofmeasurement ,
(CASE WHEN TRIM(DeactivatedFl) = '' 
THEN NULL ELSE DeactivatedFl END) AS deactivatedfl ,costelementstatusfl ,costelementshortdescr,costelementlongdescr ,
(CASE WHEN TRIM(rowstatusfl) = '' 
THEN NULL ELSE rowstatusfl END) AS rowstatusfl 
FROM mrdr_1033.tbl_CostElement""")
InsZ_INT_tbl_CostElement_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_tbl_CostElement")

#Default null handling for tbl_costcenter and storing data to InsZ_INT_tbl_costcenter
InsZ_INT_tbl_costcenter_DF = spark.sql("""
SELECT controllingarea,costcenternbr,validtodt,validfromdt,companycd,costcentercategory ,personresponsiblenm,currencykey,profitcenternbr,createdt ,enteredby,countrykey ,city ,address,postalcd ,phonenbr ,faxnbr ,hierarchyarea,costcentershortnm,costcenterlongdescr,blank_1,department ,functionalarea ,blank_2,actualprimarypostingslockind ,actualsecondarycostslockind,actualrevenuepostingslockind ,planprimarycostslockind,plansecondarycostslockind,planningrevenueslockind,blank_3,typeofworkcd ,workforcecd,enterprisefunctioncd ,pricelistfl,costingsheet ,district ,contractcd ,rowstatusfl,taxjurisdictioncd,beneficiarycd,careertrackcd,headcountfriendlyind ,economicprofilecd,economicprofiledesc,headcountdeployedind ,subcountrycd ,costlocationcd ,centertaxattributecd 
FROM mylearning_1720.InsZ_INT_tbl_costcenter""")
InsZ_INT_tbl_costcenter_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_tbl_costcenter")

#Default null handling for mrdr_1033.tbl_TypeOfWork and storing data to InsZ_INT_tbl_TypeOfWork
InsZ_INT_tbl_TypeOfWork_DF = spark.sql("""
SELECT typeofworkcd,typeofworkdescr,rowstatusfl 
FROM mrdr_1033.tbl_TypeOfWork""")
InsZ_INT_tbl_TypeOfWork_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_tbl_TypeOfWork")

#Default null handling for mrdr_1033.tbl_Company and storing data to InsZ_INT_tbl_Company
InsZ_INT_tbl_Company_DF = spark.sql("""
SELECT companycd,companydescr,city,countrykey,currencykey,bpofilterfl,blankcolumn2,postingperiodvariantcd,companyaddr,fieldstatusvar,globcompanycd,companycodeownedbycd,rowstatusfl 
FROM mylearning_1720.InsZ_INT_tbl_Company""")
InsZ_INT_tbl_Company_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_tbl_Company")

#Default null handling for mrdr_1033.tbl_ProfitCenter and storing data to InsZ_INT_tbl_ProfitCenter
InsZ_INT_tbl_ProfitCenter_DF = spark.sql("""
SELECT profitcenternbr,validtodt,controllingarea,validfromdt,createdt,enteredby,personresponsiblenm,hierarchyarea,blank_1,
(CASE WHEN TRIM(lockindicator) = '' 
THEN NULL ELSE lockindicator END) AS lockindicator,address,city,taxjurisdiction,postalcd,phonenbr,faxnbr,blank_2,profitcentershortdescr,profitcenterlongdescr,typeofwork,rowstatusfl 
FROM mrdr_1033.tbl_ProfitCenter""")
InsZ_INT_tbl_ProfitCenter_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_tbl_ProfitCenter")

#Default null handling for tbl_EnterpriseFunction and storing data to InsZ_INT_tbl_EnterpriseFunction
InsZ_INT_tbl_EnterpriseFunction_DF = spark.sql("""
SELECT enterprisefunctioncd,enterprisefunctiondescr,rowstatusfl 
FROM mrdr_1033.tbl_EnterpriseFunction""")
InsZ_INT_tbl_EnterpriseFunction_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_tbl_EnterpriseFunction")

#Default null handling for tbl_WBS and storing data to InsZ_INT_tbl_WBS
InsZ_INT_tbl_WBS_DF = spark.sql("""
SELECT wbsinternalnbr,wbsexternalnbr,
(CASE WHEN TRIM(WBSDescr) = '' 
THEN NULL ELSE WBSDescr END) AS wbsdescr,
(CASE WHEN TRIM(ProjectDefinitionNbr) = '' 
THEN NULL ELSE ProjectDefinitionNbr END) AS projectdefinitionnbr,companycd,controllingarea,profitcenternbr,projecttype,projecthierarchylevel,currencycd,wbsstats,projectprofile,userfield2,softwaremodifiedfl,partnerfunctioncd,functionalarea,billingindicator,basicstartdt,costcentercontrollingarea,basicfinishdt,createdt,lastchangedt,contractingcompanycd,wbsstatus,fs90jobprojcd,fs90gmucd,taxjurisdictioncd,rowstatusfl,industrycd,industrynm,industrysegment,
(CASE WHEN TRIM(businessactivities1) = '' 
THEN NULL ELSE businessactivities1 END) AS businessactivities1,
(CASE WHEN TRIM(businessactivities2) = '' 
THEN NULL ELSE businessactivities2 END) AS businessactivities2,
(CASE WHEN TRIM(businessactivities3) = '' 
THEN NULL ELSE businessactivities3 END) AS businessactivities3,beneficiarycompanycd,gbddopportunityid,clientfinancingfl,productivitysubcategory,productivitycategory,typeofworkcd,typeofworkdescr,
(CASE WHEN TRIM(ResponsibleCostCenter) = '' 
THEN NULL ELSE ResponsibleCostCenter END) AS responsiblecostcenter,plannedclientevapercentage,plannedcontractcipercentage,servicelinecd1,servicelinecd2,servicelinecd3,servicelinecd4,servicelinecd5,servicelinecd6,servicelinepercentage1,servicelinepercentage2,servicelinepercentage3,servicelinepercentage4,servicelinepercentage5,servicelinepercentage6,planningelementfl,detailtypeofwork1,detailtypeofwork2,detailtypeofwork3,detailtypeofworkpercentage1,detailtypeofworkpercentage2,detailtypeofworkpercentage3,intercompanycosttype,intercompanycosttypedescr,intellectualpropertyfl,microsoftalliancecd,microsoftalliancepercentage,deliverycentercd,alliancecd1,alliancerevenuepercentage1,alliancecd2,alliancerevenuepercentage2,marketlocationcd,corporatepriorities1,corporatepriorities2,corporatepriorities3,solutionunits,cosourcefl,managementunitshierarchy,managementunitsprioritiescd1,managementunitsprioritiescd2,clientgroupcategorization,accountassignmntelementfl,objectclass,resultsanalysiskey,integratedplanningfl,parentwbslvl,responsiblepartnernm,personnelnbr,employeenm,avaprofitcenternbr,federalclasscd,federalwbsind,wbsshortid,cignbr,portfoliocd,portfoliodescr,avanadeservicecd1,avanadeservicepercentage1,avanadeservicecd2,avanadeservicepercentage2,avanadeservicecd3,avanadeservicepercentage3,valuebaseddealind,revenuesharingind,allianceresaleind,subcountrycd,taxservicecategorycd,centertaxattributecd,deliveryonsiteworktxt,newprojectdocumenturltxt,facilityaliasdesc,deliverylead,multipartyfl,masterserviceagreementind,revenuerecognitiontimingcd,mmewbsforecastind,gaapcontractterm,actualgaapcontractterm 
FROM mylearning_1720.InsZ_INT_tbl_WBS""")
InsZ_INT_tbl_WBS_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_tbl_WBS")

#Default null handling for tbl_hierarchy and storing data to InsZ_INT_tbl_hierarchy
InsZ_INT_tbl_hierarchy_DF = spark.sql("""
SELECT hierarchytype,hierarchycd,entitycd,
(CASE WHEN TRIM(EntityDescr) = '' 
THEN NULL ELSE EntityDescr END) AS entitydescr,entitylev,
(CASE WHEN TRIM(ChildEntityFl) = '' 
THEN NULL ELSE ChildEntityFl END) AS childentityfl,childentitycd,
(CASE WHEN TRIM(ChildEntityDescr) = '' 
THEN NULL ELSE ChildEntityDescr END) AS childentitydescr,parententitycd,
(CASE WHEN TRIM(ParentEntityDescr) = '' 
THEN NULL ELSE ParentEntityDescr END) AS parententitydescr,rowstatusfl 
FROM mrdr_1033.tbl_hierarchy""")
InsZ_INT_tbl_hierarchy_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_tbl_hierarchy")

#Default null handling for WorkForce and storing data to insz_int_WorkForce
insz_int_WorkForce_DF = spark.sql("""
SELECT Parent_ID,Child_ID,Effective_Date,Status,
(CASE WHEN TRIM(Description) = '' 
THEN NULL ELSE Description END) AS Description,Level,
(CASE WHEN TRIM(LeadershipDesc) = '' 
THEN NULL ELSE LeadershipDesc END) AS LeadershipDescr 
FROM mrdr_1033.WorkForce""")
insz_int_WorkForce_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.insz_int_WorkForce")

#Default null handling for Empcddesc and storing data to InsZ_INT_Empcddesc
InsZ_INT_Empcddesc_DF = spark.sql("""
SELECT empcd_fk, lang_fk, name, lstupd, username, lck 
FROM mylearning_1720.Empcddesc""")
InsZ_INT_Empcddesc_DF.write.mode("overwrite").format('orc').saveAsTable("mylearning_1720_tempdb.InsZ_INT_Empcddesc")