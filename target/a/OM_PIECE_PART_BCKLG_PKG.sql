create or replace PACKAGE BODY           OM_PIECE_PART_BCKLG_PKG
AS
   /********************************************************************************************************************SYS******
            Name             : SCDH_CODE.om_piece_part_bcklg_pkg
            Purpose          : Maintaining the backlog at piece part level for all CTO and seed orders
            Object Details
               PROCEDURES
                               prc_mdm_stg_substg_condition -- This proc will give complete case statement to find stage-sub-stage of a region and order-tie-type.
                               prc_backlog_rule_setup   -- This proc will make the condition for finding backlog_facility and consumption_status
                                                           for single region,order-tie_type and sub-stage combination
                                                           using criteria defined in scdh_reporting.backlog_rule_setup table.
                               prc_bklg_fclty_con_sts_case--This proc will give complete case statement to find backlog_facility and consumption_status of a region and order-tie-type.
                               prc_populating_header_dtl-- This proc will populate the data in om_backlog_header and om_backlog_detail tables
                               prc_populating_header_dtl_opr--This proc will populate the data in om_backlog_header_stg and om_backlog_detail_stg tables
                                                               for records with rpt_doms_status='OPR'
                               prc_calculate_stg_sub_stg-- This proc will derive stg-sub-stg as well as backlog_facility and consumption_status for all orders.
                               prc_populate_om_summary--This proc will populate the data in om_backlog_summary_0609.
               FUNCTIONS
                               fn_get_condition   -- This function will make the condition for single sub-stage based upon the criteria defined in database/MDM
                               fn_get_order_source --
                               fn_get_pickup_flag --
               CURSORS
                               cur_bcklog_wo_mf
                               cur_hld_detail
                               cur_fulfillment_reg
                               cur_demand_reg

                NOTE: APJ SPECIFIC CURSORS CAN BE REMOVED AFTER 28TH JUNE 2014 ,OTM PHASE 3A DAO SUCCESSFUL RELEASE.
                      THESE SPECIFIC CURSORS WERE MADE TO HANDLE BUID,IBUID ISSUE FOR APJ IN SALES_ORDER RELATED TABLES.
                      AFTER SUCESSFUL DEPLOYMENT, APJ CAN ALSO BE HANDLED WITH GENERAL CURSOR.

            Called By        : Control-M Job
            History
           -----------------------------------------------------------------------------
            RevNo:  Date:         Author:                           Comment:
           -----------------------------------------------------------------------------
            1.0     25/11/2013     Arindam Bose/Vibha Pandey      Initial Creation.
            1.1     07/03/2015     Vibha Pandey                   Change in summary query for APJ OPR project
            1.2     21/05/2015     Thummala Padmarao              Changes in prc_populate_om_summary for PPB Phase 3 program
            1.3     11/12/2015     Vishwanatha N                  Changes for Merge Backlog requirement
            1.4     01/08/2016     Thummala Padmarao              Changes for Global Logistics Orders enablement
            1.5     01/09/2016     Arun Karthikeyan               Changes for OTM APJ Changes(Seacrh String: OTM_APJ)
            1.6     10/06/2017     Vedavalli Loganathan           Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3)
            1.7     18-Dec-2017    Abhishek Sinha                 Changes to include FGA Direct Ship Orders
            1.8     01-Jun-2018    Harish Rao                     Story#5029262 - PPB3 SCDH DP ENABLED FLAG CHANGE FROM N TO Y <HR001>
            1.9    30-Aug-2018    Rekha                      Story#5107581 - SCDH Mark as Consumed for Non-Tied Orders in Backlog <RK001>
            2.0    05-Sep-2018    Rekha                      Story#5107575 - SCDH Mark Consumed for System Tied Orders in Backlog <RK002>
           2.1  15-OCT-2018    Konda Reddy                       Story#5611255 --AMER, EMEA Logic for setting FDD Flag based upon service_sku and opr tables <KR002>
           2.2  21-Nov-2018    Rekha                             Story#5462861--OM Backlog Changes Stocking Order Profile Scenario CTO\BTO, Accessory <RK004>
           2.3   26-Nov-2018   Rekha                             Story#5500245- OM Backlog Changes PICK Order Profile Scenario CTO\BTO, Accessory on separate Tie<RK006>
           2.4   04-Jun-2019   Swetha                            Story#6741082 - Change in logic for setting FDD Flag and FDD Date based upon So_attribute table,MADB logic change
   *********************************************************************************************************************************************************/
   manifest_data_found      NUMBER (10);
   hld_data_found           NUMBER (10);
   fulfillment_data_found   NUMBER (10);
   v_order_num              VARCHAR2 (50);
   v_cnt                    NUMBER (20) := 0;
   v_ibu_id_flag            VARCHAR2 (1) := 'N';
   subscript                NUMBER := 0;
   l_error_count            NUMBER (20);
   lv_job_name              VARCHAR2 (4000);
   lv_num_of_days           NUMBER := 0;
   lv_limit                 NUMBER := 100;
   lv_loop_cnt              NUMBER := 0;
   lv_indx                  NUMBER := 0;
   lv_limit1                NUMBER
      := om_piece_part_bcklg_pkg.GET_TXN_MAX_SIZE_ROW_LIMIT (
            'SCDH_REPORTING',
            'OM_BACKLOG_HEADER_STG');
   lv_limit2                NUMBER
      := om_piece_part_bcklg_pkg.GET_TXN_MAX_SIZE_ROW_LIMIT (
            'SCDH_REPORTING',
            'OM_BACKLOG_HEADER');
   lv_limit3                NUMBER
      := om_piece_part_bcklg_pkg.GET_TXN_MAX_SIZE_ROW_LIMIT (
            'SCDH_REPORTING',
            'OM_BACKLOG_DETAIL_STG');
   lv_limit4                NUMBER
      := om_piece_part_bcklg_pkg.GET_TXN_MAX_SIZE_ROW_LIMIT (
            'SCDH_REPORTING',
            'OM_BACKLOG_DETAIL');
   lv_limit5                NUMBER
      := om_piece_part_bcklg_pkg.GET_TXN_MAX_SIZE_ROW_LIMIT (
            'SCDH_REPORTING',
            'OM_BACKLOG_SUMMARY_0609');
   lv_sync_up_id            fdl_snop_scdhub.aif_last_sync_up.sync_up_id%TYPE;
   lv_audit_log_yn          fdl_snop_scdhub.mst_sys_props.propvalue%TYPE
                               := 'N';
   lv_run_date              scdh_reporting.om_backlog_header.run_date%TYPE
                               := SYSTIMESTAMP;
   lv_job_instance_id       fdl_snop_scdhub.process_job_header.job_instance_id%TYPE;
   lv_error_location        NUMBER := 0;
   lv_error_code            VARCHAR2 (4000);
   lv_error_msg             VARCHAR2 (4000);
   header_errors            EXCEPTION;
   v_seq_num                NUMBER;
   l_fulf_region1           VARCHAR2 (100);
   l_fulf_region2           VARCHAR2 (100);
   lv_bulk_exception        EXCEPTION;
   v_sync_id                VARCHAR2 (500);
   PRAGMA EXCEPTION_INIT (lv_bulk_exception, -24381);

   TYPE exception_pos_tab
      IS TABLE OF scdh_audit.err_om_backlog_details%ROWTYPE
      INDEX BY PLS_INTEGER;

   t_excp_po                exception_pos_tab;

   TYPE CurTyp IS REF CURSOR;

   CURSOR cur_hld_detail (
      p_prd_no_num    SCDH_FULFILLMENT.BACKLOG_HEADER.PROD_ORDER_NUMBER%TYPE,
      p_region        SCDH_FULFILLMENT.BACKLOG_HEADER.REGION_CODE%TYPE)
   IS
      SELECT (SELECT LISTAGG (EVENT, ',') WITHIN GROUP (ORDER BY EVENT)
                        AS HOLD_CODE
                FROM (  SELECT PROD_ORDER_NUM,
                               EVENT,
                               ROW_NUMBER ()
                               OVER (PARTITION BY PROD_ORDER_NUM, EVENT
                                     ORDER BY PROD_ORDER_NUM)
                                  AS rn
                          FROM SCDH_FULFILLMENT.hold_event
                         WHERE     PROD_ORDER_NUM = p_prd_no_num
                               AND REGION_CODE = p_region
                      ORDER BY PROD_ORDER_NUM, EVENT)
               WHERE rn = 1)
                AS HOLD_CODE,
             (  SELECT LISTAGG (HOLD_TYPE, ',')
                          WITHIN GROUP (ORDER BY HOLD_TYPE)
                          AS HOLD_FLAG
                  FROM (  SELECT PROD_ORDER_NUM,
                                 HOLD_TYPE,
                                 ROW_NUMBER ()
                                 OVER (PARTITION BY PROD_ORDER_NUM, HOLD_TYPE
                                       ORDER BY PROD_ORDER_NUM)
                                    AS rn
                            FROM SCDH_FULFILLMENT.hold_event
                           WHERE     PROD_ORDER_NUM = p_prd_no_num
                                 AND REGION_CODE = p_region
                        ORDER BY PROD_ORDER_NUM, HOLD_TYPE)
                 WHERE rn = 1
              GROUP BY PROD_ORDER_NUM)
                AS HOLD_FLAG
        FROM DUAL;

   CURSOR cur_fulfillment_reg (
      P_ccn    SCDH_FULFILLMENT.BACKLOG_HEADER.ccn%TYPE)
   IS
      SELECT propvalue AS FULFILLMENT_REGION_CODE
        FROM fdl_snop_scdhub.mst_sys_props
       WHERE propname = P_CCN AND id = 'FULFILLMENT_REGION_CODE';

   CURSOR cur_update_thread_status (
      p_job_name    FDL_SNOP_SCDHUB.PROCESS_JOB.job_name%TYPE)
   IS
          SELECT START_SEQ_NUM
            FROM FDL_SNOP_SCDHUB.AIF_LAST_SYNC_UP
           WHERE sync_up_id = p_job_name
      FOR UPDATE OF START_SEQ_NUM;

   FUNCTION fn_demand_supply_reg (
      p_cons_facility    SCDH_FULFILLMENT.BACKLOG_DETAIL.CONSUMPTION_FACILITY%TYPE)
      RETURN VARCHAR2
   AS
      v_cons_facility   VARCHAR2 (10);
   BEGIN
      SELECT fsr.region_code demand_supply_region_code
        INTO v_cons_facility
        FROM scdh_master.facility f, scdh_master.FF_SUB_REGION_SITE_XREF fsr
       WHERE     f.facility_code = p_cons_facility
             AND f.site_name = fsr.site_name
             AND fsr.ff_type_name = 'DEMAND-SUPPLY'
             AND f.SYS_ENT_STATE = 'ACTIVE'
             AND fsr.SYS_ENT_STATE = 'ACTIVE';

      RETURN v_cons_facility;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN NULL;
      WHEN OTHERS
      THEN
         IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
               P_STEP_NAME                 => 'SELECT',
               P_JOB_AUDIT_MESSAGE         =>    'CALCULATION FOR DEMAND SUPPLY REGION FOR p_cons_facility- '
                                              || p_cons_facility,
               P_NUMBER_OF_ROWS_SELECTED   => NULL,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => NULL,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_IS_BASE_TABLE             => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'Calculation started..',
               P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
               P_JOB_INSTANCE_ID           => lv_job_instance_id,
               P_JOB_NAME                  => lv_job_name,
               P_JOB_AUDIT_LEVEL_ID        => NULL,
               P_COMMAND_NAME              => 'SQL',
               P_COMMAND_TYPE              => 'SELECT',
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);

            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
               P_NUMBER_OF_ROWS_SELECTED   => NULL,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => NULL,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           =>    'CALCULATION FOR DEMAND SUPPLY REGION FOR p_cons_facility- '
                                              || p_cons_facility,
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);
         END IF;

         RETURN NULL;
   END fn_demand_supply_reg;

   ------------------------This function will make the condition for single sub-stage based upon the criteria defined in database/MDM-------------------------------
   -------------------------Input : Ref cursor containing all rows which will make complete condition for single sub-stage--------------------------------------
   ------------------------Output : condition  ex. (DOMS_STATUS='IP' OR DOMS_STATUS='PP') AND ASN_STATUS_CODE='35'---------------------------
   FUNCTION FN_GET_CONDITION (p_cur IN SYS_REFCURSOR)
      RETURN CLOB
   AS
      v_priority_prev     scdh_reporting.backlog_rule_setup.priority%TYPE;
      condition           CLOB;                            --VARCHAR2 (10000);
      v_cnt_priority      NUMBER;
      v_criteria_exists   BOOLEAN := TRUE;

      TYPE rec_criteria IS RECORD
      (
         priority             scdh_reporting.backlog_rule_setup.priority%TYPE,
         criteria_attribute   scdh_reporting.backlog_rule_setup.criteria_attribute%TYPE,
         operators            scdh_reporting.backlog_rule_setup.operators%TYPE,
         criteria_value       scdh_reporting.backlog_rule_setup.criteria_value%TYPE
      );

      TYPE cur_tab IS TABLE OF rec_criteria;

      T_cur_tab           cur_tab;
   BEGIN
      v_priority_prev := '';
      condition := NULL;

      FETCH P_CUR BULK COLLECT INTO T_cur_tab;

      IF T_cur_tab.COUNT > 0
      THEN
         FOR i IN T_cur_tab.FIRST .. T_cur_tab.LAST
         LOOP
            IF condition IS NULL
            THEN
               IF T_cur_tab (I).criteria_attribute IS NULL
               THEN
                  v_criteria_exists := FALSE;
               ELSE
                  v_criteria_exists := TRUE;
               END IF;
            END IF;

            IF T_cur_tab (I).priority = v_priority_prev
            THEN
               condition :=
                     condition
                  || ' AND '
                  || T_cur_tab (I).criteria_attribute
                  || ' '
                  || T_cur_tab (I).operators
                  || ' '
                  || T_cur_tab (I).criteria_value;
            ELSE
               IF i = 1
               THEN
                  condition :=
                        '( '
                     || T_cur_tab (I).criteria_attribute
                     || ' '
                     || T_cur_tab (I).operators
                     || ' '
                     || T_cur_tab (I).criteria_value;
               ELSE
                  condition :=
                        condition
                     || ')'
                     || ' OR '
                     || '( '
                     || T_cur_tab (I).criteria_attribute
                     || ' '
                     || T_cur_tab (I).operators
                     || ' '
                     || T_cur_tab (I).criteria_value;
               END IF;
            END IF;

            v_priority_prev := T_cur_tab (I).priority;
            v_cnt_priority := i;
         END LOOP;
      ELSE
         RETURN 'NULL';
      END IF;

      IF v_cnt_priority = 1
      THEN
         condition := condition || ')';
      ELSE
         condition := '(' || condition || ')' || ')';
      END IF;

      CLOSE P_CUR;

      IF v_criteria_exists
      THEN
         RETURN condition;
      ELSE
         RETURN 'NULL';
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN 'NULL';
   END;

   ---------------------This proc will give complete case statement to find stage-sub-stage of a region and order-tie-type.---------------------------
   /*This proc will call  FN_GET_CONDITION and will get the condition for every sub-stage.Then it will make a complete case statement for single region and
   order-tie-type by these conditions.This proc will store region,order-tie-type and corresponding case statement in t_stg_substg_case_stmt(session variable)*/
   PROCEDURE PRC_MDM_STG_SUBSTG_CONDITION (
      p_region         scdh_master.ord_cyl_stg_substg_asgmt.region%TYPE,
      p_err_code   OUT VARCHAR2,
      p_err_mesg   OUT VARCHAR2)
   AS
      t_stg_substg_condition   scdh_reporting.tab_of_condition
                                  := scdh_reporting.tab_of_condition ();
      v_sql                    CLOB;                       --VARCHAR2 (30000);
      v_result                 CLOB;                       --VARCHAR2 (10000);
      l_refcursor              SYS_REFCURSOR;
   BEGIN
      --DBMS_OUTPUT.PUT_LINE(l_fulf_region1||l_fulf_region2);
      FOR j
         IN (  SELECT region,
                      order_tie_type,
                      CYL_SUB_STG_SEQUENCE stage_sequence,
                      cycle_stage,
                      cycle_sub_stage
                 FROM scdh_master.ord_cyl_stg_substg_asgmt asmt
                      INNER JOIN scdh_master.order_cycle_substage stg
                         ON     asmt.cycle_stage = stg.cycle_stage_code
                            AND asmt.cycle_sub_stage = stg.cycle_substage_code
                WHERE     asmt.sys_ent_state = 'ACTIVE'
                      AND stg.sys_ent_state = 'ACTIVE'
                      AND asmt.region IN (l_fulf_region1, l_fulf_region2)
             GROUP BY region,
                      order_tie_type,
                      CYL_SUB_STG_SEQUENCE,
                      cycle_stage,
                      cycle_sub_stage)
      LOOP
         OPEN l_refcursor FOR
              SELECT PRIORITY,
                     CRITERIA_ATTRIBUTE,
                     OPERATORS,
                     CRITERIA_VALUE
                FROM SCDH_MASTER.ORD_CYL_STG_SUBSTG_ASGMT
               WHERE     REGION = j.REGION
                     AND ORDER_TIE_TYPE = j.ORDER_TIE_TYPE
                     AND CYCLE_STAGE = j.CYCLE_STAGE
                     AND CYCLE_SUB_STAGE = j.CYCLE_SUB_STAGE
                     AND SYS_ENT_STATE = 'ACTIVE'
            ORDER BY PRIORITY, CRITERIA_ATTRIBUTE;

         -------------------------Get condition for every stage and sub stage--------------------------------------------
         v_result := FN_GET_CONDITION (l_refcursor);

         t_stg_substg_condition.EXTEND;
         t_stg_substg_condition (t_stg_substg_condition.LAST) :=
            NEW scdh_reporting.obj_of_condition (j.region,
                                                 j.order_tie_type,
                                                 j.stage_sequence,
                                                 j.cycle_stage,
                                                 j.cycle_sub_stage,
                                                 v_result);
      END LOOP;

      --DBMS_OUTPUT.PUT_LINE('CONDITION');
      --------------------------------------------------Make complete case statement------------------------------------------------------------------
      FOR i
         IN (SELECT DISTINCT region_code, order_tie_type
               FROM TABLE (t_stg_substg_condition))
      LOOP
         v_sql := NULL;

         --DBMS_OUTPUT.PUT_LINE(I.region_code||I.order_tie_type);
         FOR j
            IN (  SELECT region_code,
                         order_tie_type,
                         stage,
                         substage,
                         condition
                    FROM TABLE (t_stg_substg_condition) t
                   WHERE     t.region_code = i.region_code
                         AND t.order_tie_type = i.order_tie_type
                ORDER BY t.stage_sequence DESC)
         LOOP
            IF v_sql IS NULL
            THEN
               v_sql :=
                     'SELECT CASE WHEN '
                  || j.condition
                  || ' THEN '''
                  || j.stage
                  || '#'
                  || j.substage
                  || '''';
            ELSE
               v_sql :=
                     v_sql
                  || ' WHEN '
                  || j.condition
                  || ' THEN '''
                  || j.stage
                  || '#'
                  || j.substage
                  || '''';
            END IF;
         END LOOP;

         --DBMS_OUTPUT.PUT_LINE(I.region_code||I.order_tie_type||LENGTH(v_sql));
         v_sql :=
               v_sql
            || 'ELSE ''UNK#UNK'' END ss FROM scdh_reporting.OM_BACKLOG_HEADER_STG S where order_num=:ORDER_NUMBER AND PROD_ORDER_NUM=:PROD_ORDER_NUMBER AND TIE_NUMBER=:TIE_NUMBER AND REGION_CODE=:REGION_CODE and bu_id=:bu_id AND mod_num=:mod_num';
         t_stg_substg_case_stmt.EXTEND;
         t_stg_substg_case_stmt (t_stg_substg_case_stmt.LAST) :=
            NEW scdh_reporting.obj_of_case_stmt (i.region_code,
                                                 i.order_tie_type,
                                                 v_sql);
        --  dbms_output.put_line('v_sql in prc_mdm:'||v_sql);
      END LOOP;
   EXCEPTION
      WHEN OTHERS
      THEN
         --DBMS_OUTPUT.PUT_LINE(SQLERRM);
         p_err_code := SQLCODE;
         p_err_mesg :=
            SUBSTR (
                  lv_job_name
               || ' Error in PRC_MDM_STG_SUBSTG_CONDITION'
               || SQLERRM,
               1,
               3500);
   END PRC_MDM_STG_SUBSTG_CONDITION;

   /*This proc will make the condition for finding backlog_facility and consumption_status for single region,order-tie_type and sub-stage combination
                                           using criteria defined in scdh_reporting.backlog_rule_setup table*/
   -------------------------------Input: deriving_pararmeter ex. 'CONSUMPTION_STATUS','BACKLOG_FACILITY'----------------------------
   -------------------------------Output: P_tAB_backlog_rule_condition --Table of complete conditions.
   PROCEDURE prc_backlog_rule_setup (
      p_deriving_parameter           IN     VARCHAR2,
      P_tAB_backlog_rule_condition   IN OUT SCDH_REPORTING.tab_of_condition,
      p_region                       IN     scdh_reporting.backlog_rule_setup.REGION%TYPE,
      p_err_code                        OUT VARCHAR2,
      p_err_mesg                        OUT VARCHAR2)
   AS
      l_refcursor   SYS_REFCURSOR;
      v_result      CLOB;
   BEGIN
      FOR j
         IN (  SELECT region,
                      order_tie_type,
                      cycle_sub_stage,
                      CYL_SUB_STG_SEQUENCE sequence,
                      DECODE (NVL (result_column, 'NULL'),
                              'NULL', '''' || result_value || '''',
                              result_column)
                         result_col
                 FROM scdh_reporting.backlog_rule_setup rul
                      INNER JOIN scdh_master.order_cycle_substage stg
                         ON rul.cycle_sub_stage = stg.cycle_substage_code
                WHERE     deriving_parameter = p_deriving_parameter
                      AND rul.SYS_ENT_STATE = 'ACTIVE'
                      AND stg.sys_ent_state = 'ACTIVE'
                      AND rul.region IN (l_fulf_region1, l_fulf_region2)
             GROUP BY region,
                      order_tie_type,
                      cycle_sub_stage,
                      CYL_SUB_STG_SEQUENCE,
                      DECODE (NVL (result_column, 'NULL'),
                              'NULL', '''' || result_value || '''',
                              result_column)
             ORDER BY CYL_SUB_STG_SEQUENCE)
      LOOP
         OPEN l_refcursor FOR
              SELECT priority,
                     CRITERIA_ATTRIBUTE,
                     OPERATORS,
                     CRITERIA_VALUE
                FROM scdh_reporting.backlog_rule_setup
               WHERE     region = j.region
                     AND order_tie_type = j.order_tie_type
                     AND cycle_sub_stage = j.cycle_sub_stage
                     AND DECODE (NVL (result_column, 'NULL'),
                                 'NULL', '''' || result_value || '''',
                                 result_column) = j.result_col
                     AND deriving_parameter = p_deriving_parameter
                     AND SYS_ENT_STATE = 'ACTIVE'
            ORDER BY priority;

         v_result := FN_GET_CONDITION (l_refcursor);

         IF v_result != 'NULL'
         THEN
            v_result :=
                  'WHEN :cycle_substage_code = '''
               || j.cycle_sub_stage
               || ''' AND '
               || v_result
               || ' THEN '
               || j.result_col;
         ELSE
            v_result :=
                  'WHEN :cycle_substage_code = '''
               || j.cycle_sub_stage
               || ''' THEN '
               || j.result_col;
         END IF;

         P_tAB_backlog_rule_condition.EXTEND;
         P_tAB_backlog_rule_condition (P_tAB_backlog_rule_condition.LAST) :=
            NEW scdh_reporting.obj_of_condition (j.region,
                                                 j.order_tie_type,
                                                 j.sequence,
                                                 NULL,
                                                 j.cycle_sub_stage,
                                                 v_result);
      END LOOP;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_err_code := SQLCODE;
         p_err_mesg :=
               'Error in Backlog rules,Check prc_backlog_rule_setup  '
            || SUBSTR (SQLERRM, 1, 3500);
   END prc_backlog_rule_setup;

   ------------/*This proc will give complete case statement to find backlog_facility and consumption_status of a region and order-tie-type.*/ --------
   -------------Input : P_backlog_FCLTY_condition : Table of conditions for backlog_facility----------------
   -------------------- P_CON_STS_condition : Table of conditions for consumption_status---------------------
   -------------Output : t_backlog_CON_STS_case_stmt : Table of case statements (session variable)-----------------------
   PROCEDURE PRC_BKLG_FCLTY_CON_STS_CASE (
      P_backlog_FCLTY_condition       SCDH_REPORTING.tab_of_condition,
      P_CON_STS_condition             SCDH_REPORTING.tab_of_condition,
      p_mod                           SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE, --
      p_err_code                  OUT VARCHAR2,
      p_err_mesg                  OUT VARCHAR2)
   AS
      v_sql_bklg_fclty   CLOB;                              --VARCHAR2 (5000);
      v_sql_cons_sts     CLOB;                              --VARCHAR2 (5000);
      v_sql              CLOB;                              --VARCHAR2 (5000);
   BEGIN
      FOR i IN (  SELECT region_code, order_tie_type
                    FROM TABLE (P_backlog_FCLTY_condition)
                GROUP BY region_code, order_tie_type)
      LOOP
         v_sql_bklg_fclty := NULL;
         v_sql_cons_sts := NULL;

         FOR j
            IN (  SELECT *
                    FROM TABLE (P_backlog_FCLTY_condition) t
                   WHERE     t.region_code = i.region_code
                         AND t.order_tie_type = i.order_tie_type
                ORDER BY t.stage_sequence DESC)
         LOOP
            IF v_sql_bklg_fclty IS NULL
            THEN
               v_sql_bklg_fclty := 'SELECT CASE ' || j.condition;
            ELSE
               v_sql_bklg_fclty := v_sql_bklg_fclty || ' ' || j.condition;
            END IF;
         END LOOP;

         v_sql_bklg_fclty := v_sql_bklg_fclty || ' END AS BACKLOG_FACILITY';

         FOR k
            IN (  SELECT *
                    FROM TABLE (P_CON_STS_condition) t
                   WHERE     t.region_code = i.region_code
                         AND t.order_tie_type = i.order_tie_type
                ORDER BY t.stage_sequence DESC)
         LOOP
            IF v_sql_cons_sts IS NULL
            THEN
               v_sql_cons_sts := 'CASE ' || K.condition;
            ELSE
               v_sql_cons_sts := v_sql_cons_sts || ' ' || K.condition;
            END IF;
         END LOOP;

         --  AND BH.MOD_NUM = BD.MOD_NUM  AND BH.MOD_NUM = :mod_num
         v_sql :=
               v_sql_bklg_fclty
            || ' , '
            || v_sql_cons_sts
            || ' END AS CONSUMPTION_STATUS,MOD_PART_NUMBER,PART_NUMBER,DTL_SEQ_NUM,bd.rowid FROM SCDH_REPORTING.OM_BACKLOG_HEADER_STG bh
            join SCDH_REPORTING.OM_BACKLOG_DETAIL_STG bd
            on bh.region_code=bd.region_code
            and bh.order_num=bd.order_num
            and bh.prod_order_num=bd.prod_order_num
            and bh.tie_number=bd.tie_number
            and bh.bu_id=bd.bu_id
            and bh.job_time=bd.job_time
            AND BH.MOD_NUM = BD.MOD_NUM
            WHERE bh.REGION_CODE=:region_code
            AND BH.ORDER_NUM=:ORDER_NUM
            AND BH.PROD_ORDER_NUM=:PROD_ORDER_NUM
            AND BH.TIE_NUMBER=:TIE_NUMBER
            AND BH.bu_id=:bu_id
            AND BH.JOB_TIME=''HOURLY''
            AND BH.MOD_NUM = :mod_num
            AND is_supressed=''N''';
         t_backlog_CON_STS_case_stmt.EXTEND;
         t_backlog_CON_STS_case_stmt (t_backlog_CON_STS_case_stmt.LAST) :=
            NEW scdh_reporting.obj_of_case_stmt (i.region_code,
                                                 i.order_tie_type,
                                                 v_sql);

      END LOOP;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_err_code := SQLCODE;
         p_err_mesg :=
            SUBSTR ('Check PRC_BKLG_FCLTY_CON_STS_CASE ' || SQLERRM, 1, 3500);
   END PRC_BKLG_FCLTY_CON_STS_CASE;

   ---------------/*This will return case statement for given region,order_tie_type*/--------------------------
   ---------------P_TABLE_NAME : This will define table name from which we need case statement.ex. t_backlog_CON_STS_case_stmt , t_stg_substg_case_stmt
   FUNCTION fn_get_case_stmt (
      p_region                   IN SCDH_REPORTING.OM_BACKLOG_HEADER_STG.REGION_CODE%TYPE,
      p_backlog_order_tie_type   IN SCDH_REPORTING.OM_BACKLOG_HEADER_STG.backlog_order_tie_type%TYPE,
      P_TABLE_NAME                  scdh_reporting.TAB_OF_CASE_STMT)
      RETURN CLOB                                               --RESULT_CACHE
   AS
      v_sql   CLOB;
   BEGIN
      --dbms_output.put_line(p_region||p_backlog_order_tie_type);
      SELECT t.case_stmt
        INTO v_sql
        FROM TABLE (P_TABLE_NAME) t
       WHERE     region_code = p_region
             AND order_tie_type = p_backlog_order_tie_type;
       --dbms_output.put_line('v_sql:'||v_sql);
      RETURN v_sql;
   EXCEPTION
      WHEN OTHERS
      THEN
         IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
               P_STEP_NAME                 => 'SELECT',
               P_JOB_AUDIT_MESSAGE         =>    'CALCULATION FOR get case statement- '
                                              || p_region
                                              || ' tie type '
                                              || p_backlog_order_tie_type,
               P_NUMBER_OF_ROWS_SELECTED   => NULL,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => NULL,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_IS_BASE_TABLE             => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'Calculation started..',
               P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
               P_JOB_INSTANCE_ID           => lv_job_instance_id,
               P_JOB_NAME                  => lv_job_name,
               P_JOB_AUDIT_LEVEL_ID        => NULL,
               P_COMMAND_NAME              => 'SQL',
               P_COMMAND_TYPE              => 'SELECT',
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);

            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
               P_NUMBER_OF_ROWS_SELECTED   => NULL,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => NULL,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'fn_get_case_statement failed',
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);
         END IF;
         dbms_output.put_line('error in fn_get_case_stmt:'||sqlerrm);

         v_sql := 'UNK'; --||substr(sqlerrm,1,200);
         RETURN v_sql;
   END fn_get_case_stmt;

   ---------FUNCTION TO BRING BOTH AI_DOMS_ORDER AND LOCAL_CHANNEL--------------
   FUNCTION get_ai_doms_ord (
      p_order_id    SCDH_fulfillment.sales_order_ai.SALES_ORDER_ID%TYPE,
      p_bu_id       SCDH_fulfillment.sales_order_ai.BUID%TYPE)
      RETURN r_ai_dom_lcl_chnl
   AS
      lv_error_code           VARCHAR2 (4000);
      lv_error_msg            VARCHAR2 (4000);
      lv_error_location       NUMBER := 0;
      lv_tot_rec_cnt          NUMBER := 0;
      lv_selcted_rec          NUMBER := 0;
      lv_insert_rec           NUMBER := 0;
      lv_updated_rec          NUMBER := 0;
      lv_deletd_rec           NUMBER := 0;
      lv_job_audit_log_id_1   fdl_snop_scdhub.process_job_detail.job_audit_log_id%TYPE;
      rec_doms                r_ai_dom_lcl_chnl;
   --      lv_job_audit_log_id_1           fdl_snop_scdhub.process_job_detail.job_audit_log_id%
   BEGIN
      -- lv_job_name := 'OM_BACKLOG_HD_DETAIL_AMER';
      SELECT AI_SALES_ORDER_ID, NULL customer_class ---modified to NULL temporarily
        INTO rec_doms
        FROM SCDH_fulfillment.sales_order_ai
       WHERE SALES_ORDER_ID = p_order_id AND BUID = p_bu_id;

      RETURN rec_doms;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         rec_doms.ai_doms_id := NULL;
         rec_doms.local_channel := NULL;
         RETURN rec_doms;
      WHEN OTHERS
      THEN
         rec_doms.ai_doms_id := NULL;
         rec_doms.local_channel := NULL;

         IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
               P_STEP_NAME                 => 'SELECT',
               P_JOB_AUDIT_MESSAGE         =>    'CALCULATION FOR AI_DOMS FIELDS FOR ORDER- '
                                              || p_order_id,
               P_NUMBER_OF_ROWS_SELECTED   => NULL,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => NULL,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_IS_BASE_TABLE             => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'Calculation started..',
               P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
               P_JOB_INSTANCE_ID           => lv_job_instance_id,
               P_JOB_NAME                  => lv_job_name,
               P_JOB_AUDIT_LEVEL_ID        => NULL,
               P_COMMAND_NAME              => 'SQL',
               P_COMMAND_TYPE              => 'SELECT',
               P_JOB_AUDIT_LOG_ID          => lv_job_audit_log_id_1,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);

            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
               P_NUMBER_OF_ROWS_SELECTED   => NULL,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => NULL,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           =>    'AI_DOMS FAILED FOR ORDER '
                                              || p_order_id,
               P_JOB_AUDIT_LOG_ID          => lv_job_audit_log_id_1,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);
         END IF;

         RETURN rec_doms;
   END;

   -------------------------This proc will derive stg-sub-stg as well as backlog_facility and consumption_status for all orders.---------------
   PROCEDURE prc_calculate_stg_sub_stg (
      p_region_code   IN     SCDH_REPORTING.OM_BACKLOG_HEADER.REGION_CODE%TYPE,
      p_mod           IN     SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE,
      p_err_code         OUT VARCHAR2,
      p_err_mesg         OUT VARCHAR2)
   AS
      -----------------------CURSOR TO GET KEY COLUMNS FOR UPDATING STAGE AND SUB-STAGE----------------------------------
      CURSOR cur_get_header_data_for_stg (
         p_region_code    SCDH_REPORTING.OM_BACKLOG_HEADER.REGION_CODE%TYPE,
         p_mod            SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE) --
      IS
           SELECT ROWID,
                  order_num,
                  prod_order_num,
                  tie_number,
                  bu_id,
                  region_code,
                  cycle_stage_code,
                  cycle_substage_code,
                  backlog_order_tie_type,
                  SHIP_TO_FACILITY,
                  TP_FACILITY,
                  SOURCE_LOCAL_CHANNEL_CODE,
                  BUILD_TYPE,
                  ASN_ID,
                  ASN_STATUS_CODE,
                  WO_STATUS_CODE,
                  ship_notification_date,
                  channel_status_code,
                  HOLD_CODE,
                  HOLD_FLAG,
                  FULFILLMENT_REGION_CODE,
                  SYSTEM_TYPE_CODE,
                  SSC_NAME,
                  ai_doms_order_id,
                  pick_make,
                  ORDER_SOURCE,
                  Fulf_Order_Type,
                  CCN,
                  mod_num,
                  FGA_DIRECT_SHIP_FLAG,    -- Abhishek Added for IsFGADirectShip
                 -- merge_type,  --<RK004>
                  om_order_type,  --<RK004>
                  rsid --<RK004>
             FROM SCDH_REPORTING.OM_BACKLOG_HEADER_STG
            WHERE region_code = p_region_code AND mod_num = p_mod --and order_num = '396714338'
         ORDER BY order_num;

      TYPE get_header_data_for_stg_TAB
         IS TABLE OF cur_get_header_data_for_stg%ROWTYPE;

      TYPE REC_detail_get_pk_data IS RECORD
      (
         backlog_facility     SCDH_REPORTING.OM_BACKLOG_DETAIL.backlog_facility%TYPE,
         consumption_status   SCDH_REPORTING.OM_BACKLOG_DETAIL.consumption_status%TYPE,
         mod_part_number      SCDH_REPORTING.OM_BACKLOG_DETAIL.mod_part_number%TYPE,
         part_number          SCDH_REPORTING.OM_BACKLOG_DETAIL.part_number%TYPE,
         DTL_SEQ_NUM          SCDH_REPORTING.OM_BACKLOG_DETAIL.DTL_SEQ_NUM%TYPE,
         VROWID               ROWID
      );

      TYPE TAB_detail_get_pk_data IS TABLE OF REC_detail_get_pk_data;

      T_detail_get_pk_data            TAB_detail_get_pk_data
                                         := TAB_detail_get_pk_data ();

      TYPE REC_detail_bklg_fclty_con_sts IS RECORD
      (
         region_code          SCDH_REPORTING.OM_BACKLOG_DETAIL.region_code%TYPE,
         bu_id                SCDH_REPORTING.OM_BACKLOG_DETAIL.bu_id%TYPE,
         ORDER_NUM            SCDH_REPORTING.OM_BACKLOG_DETAIL.ORDER_NUM%TYPE,
         prod_ORDER_NUM       SCDH_REPORTING.OM_BACKLOG_DETAIL.prod_ORDER_NUM%TYPE,
         tie_number           SCDH_REPORTING.OM_BACKLOG_DETAIL.tie_number%TYPE,
         DTL_SEQ_NUM          SCDH_REPORTING.OM_BACKLOG_DETAIL.DTL_SEQ_NUM%TYPE,
         mod_part_number      SCDH_REPORTING.OM_BACKLOG_DETAIL.mod_part_number%TYPE,
         part_number          SCDH_REPORTING.OM_BACKLOG_DETAIL.part_number%TYPE,
         backlog_facility     SCDH_REPORTING.OM_BACKLOG_DETAIL.backlog_facility%TYPE,
         consumption_status   SCDH_REPORTING.OM_BACKLOG_DETAIL.consumption_status%TYPE,
         VROWID               ROWID
      );

      TYPE TAB_detail_bklg_fclty_con_sts
         IS TABLE OF REC_detail_bklg_fclty_con_sts;

      T_detail_bklg_fclty_con_sts     TAB_detail_bklg_fclty_con_sts
                                         := TAB_detail_bklg_fclty_con_sts ();

      t_get_header_data_for_stg_TAB   get_header_data_for_stg_TAB;

      TYPE hld_detail_tab IS TABLE OF cur_hld_detail%ROWTYPE
         INDEX BY PLS_INTEGER;

      t_hld_detail_tab                hld_detail_tab;

      TYPE coll_fulfillment_reg IS TABLE OF cur_fulfillment_reg%ROWTYPE
         INDEX BY PLS_INTEGER;

      t_fulfillment_reg_tab           coll_fulfillment_reg;

      v_case_sql                      VARCHAR2 (32767);
      v_BKLG_FCLTY_CON_STS_sql        VARCHAR2 (5000);
      v_stage_sub_stg                 VARCHAR2 (500);
      v_cursor                        BINARY_INTEGER;
      v_execute                       BINARY_INTEGER;
      lref_cursor                     SYS_REFCURSOR;
      temp                            VARCHAR2 (500);
      lv_error_code                   VARCHAR2 (4000);
      lv_error_msg                    VARCHAR2 (4000);
      lv_error_location               NUMBER := 0;
      lv_tot_rec_cnt                  NUMBER := 0;
      lv_selcted_rec                  NUMBER := 0;
      lv_insert_rec                   NUMBER := 0;
      lv_updated_rec                  NUMBER := 0;
      lv_deletd_rec                   NUMBER := 0;
      lv_indx                         NUMBER := 0;
      lv_indx_1                       NUMBER := 0;
      lv_cnt                          NUMBER := 0;
      lv_result                       BOOLEAN;
      exp_post                        EXCEPTION;
      lv_job_audit_log_id_1           fdl_snop_scdhub.process_job_detail.job_audit_log_id%TYPE;
      lv_job_audit_log_id_2           fdl_snop_scdhub.process_job_detail.job_audit_log_id%TYPE;
      v_chk_order                     SCDH_REPORTING.OM_BACKLOG_HEADER.ORDER_NUM%TYPE
         := '0';
      v_ai_doms_order_id              scdh_reporting.OM_BACKLOG_HEADER.AI_DOMS_ORDER_ID%TYPE;
      v_lcl_chnl_code                 scdh_reporting.OM_BACKLOG_HEADER.SOURCE_LOCAL_CHANNEL_CODE%TYPE;
      V_ORDER_SOURCE                  scdh_reporting.OM_BACKLOG_HEADER.ORDER_SOURCE%TYPE;
      v_ful_order_type                scdh_reporting.OM_BACKLOG_HEADER.Fulf_Order_Type%TYPE;
   BEGIN
      lv_error_location := 1.1;

      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
            P_STEP_NAME                 => 'SELECT',
            P_JOB_AUDIT_MESSAGE         =>    'CALCULATION FOR OTHER FIELDS FOR - '
                                           || p_region_CODE,
            P_NUMBER_OF_ROWS_SELECTED   => NULL,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_IS_BASE_TABLE             => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'Calculation started..',
            P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
            P_JOB_INSTANCE_ID           => lv_job_instance_id,
            P_JOB_NAME                  => lv_job_name,
            P_JOB_AUDIT_LEVEL_ID        => NULL,
            P_COMMAND_NAME              => 'SQL',
            P_COMMAND_TYPE              => 'SELECT',
            P_JOB_AUDIT_LOG_ID          => lv_job_audit_log_id_1,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            p_err_code := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      lv_tot_rec_cnt := 0;

      OPEN cur_get_header_data_for_stg (p_region_code, p_mod);              --

      LOOP
         lv_error_location := 1.2;

         FETCH cur_get_header_data_for_stg
            BULK COLLECT INTO t_get_header_data_for_stg_tab
            LIMIT lv_limit1;

         EXIT WHEN t_get_header_data_for_stg_tab.COUNT = 0;

         FOR i IN t_get_header_data_for_stg_tab.FIRST ..
                  t_get_header_data_for_stg_tab.LAST
         LOOP
            BEGIN
               IF v_chk_order <> t_get_header_data_for_stg_tab (i).ORDER_NUM
               THEN
                  lv_error_location := 1.3;
                  v_chk_order := t_get_header_data_for_stg_tab (i).ORDER_NUM;
                  -----------------------------------calculate remaining fields for header----------------------------------------------------------------
                  --------------------------------FETCHED HOLD CODE AND HELD FLAG FOR 1st PO NUMBER FOR EACH ORDER---------------------------------------------
                  ---------------------------This is at order level not at tie level so calculating once only for all ties---------------------------------------
                  lv_error_location := 1.4;

                  OPEN cur_hld_detail (t_get_header_data_for_stg_tab (i).PROD_ORDER_NUM,
                                       p_region_code);

                  FETCH cur_hld_detail BULK COLLECT INTO t_hld_detail_tab;

                  -------------------------------FETCHED FULFILLMENT/DEMAND SUPPLY LOCATION DETAILS ------------------------------------------------------------------
                  ---------------------------This is at order level not at tie level so calculating once only for all ties---------------------------------------
                  lv_error_location := 1.5;

                  OPEN cur_fulfillment_reg (t_get_header_data_for_stg_tab (i).CCN);

                  FETCH cur_fulfillment_reg
                     BULK COLLECT INTO t_fulfillment_reg_tab;

                  IF t_fulfillment_reg_tab.COUNT = 0
                  THEN
                     t_fulfillment_reg_tab (1).FULFILLMENT_REGION_CODE :=
                        CASE
                           WHEN t_get_header_data_for_stg_tab (i).REGION_CODE =
                                   'AMER'
                           THEN
                              'DAO'
                           ELSE
                              t_get_header_data_for_stg_tab (i).REGION_CODE
                        END;
                  END IF;

                  ------------------------------AI_DOMS AND LOCAL CHANNEL IN THIS RECORD TYPE---------------------------
                  ---------------------------This is at order level not at tie level so calculating once only for all ties---------------------------------------
                  lv_error_location := 1.6;
                  rec1_doms :=
                     get_ai_doms_ord (
                        t_get_header_data_for_stg_tab (i).ORDER_NUM,
                        t_get_header_data_for_stg_tab (i).BU_ID);

                  IF (    t_get_header_data_for_stg_tab (i).BU_ID = '11'
                      AND t_get_header_data_for_stg_tab (i).REGION_CODE =
                             'AMER')
                  THEN
                     v_ai_doms_order_id := rec1_doms.ai_doms_id;
                  ELSE
                     v_ai_doms_order_id := NULL;
                  END IF;

                  v_lcl_chnl_code :=
                     t_get_header_data_for_stg_tab (i).SOURCE_LOCAL_CHANNEL_CODE;
                  --------------------------------------------DERIVING ORDER_SOURCE -----------------------------------------------------------------------------
                  ---------------------------This is at order level not at tie level so calculating once only for all ties---------------------------------------
                  lv_error_location := 1.7;

                  IF     t_get_header_data_for_stg_tab (i).REGION_CODE =
                            p_region_code
                     AND t_get_header_data_for_stg_tab (i).BUILD_TYPE =
                            'BUILDTOSTOCK'
                  THEN
                     V_ORDER_SOURCE := 'OMS-SEED';
                     v_ful_order_type := 'SUPPLY';
                  ELSIF fn_get_order_source (
                           t_get_header_data_for_stg_tab (i).BU_ID,
                           v_lcl_chnl_code) = 'OMS-SEED'
                  THEN
                     V_ORDER_SOURCE := 'OMS-SEED';
                     v_ful_order_type := 'SUPPLY';
                  ELSE
                     V_ORDER_SOURCE := 'OMS-CUSTOMER';
                     v_ful_order_type := 'CUSTOMER';
                  END IF;
               END IF;

               ------------------------------------------Assign calculated values------------------------------------------------------------------
               IF (t_hld_detail_tab.COUNT <> 0)
               THEN
                  t_get_header_data_for_stg_tab (i).HOLD_CODE :=
                     t_hld_detail_tab (1).HOLD_CODE;
                  t_get_header_data_for_stg_tab (i).HOLD_FLAG :=
                     t_hld_detail_tab (1).HOLD_FLAG;
               END IF;

               t_get_header_data_for_stg_tab (i).FULFILLMENT_REGION_CODE :=
                  t_fulfillment_reg_tab (1).FULFILLMENT_REGION_CODE;

               t_get_header_data_for_stg_tab (i).ai_doms_order_id :=
                  v_ai_doms_order_id;
               t_get_header_data_for_stg_tab (i).SOURCE_LOCAL_CHANNEL_CODE :=
                  v_lcl_chnl_code;
               t_get_header_data_for_stg_tab (i).ORDER_SOURCE :=
                  V_ORDER_SOURCE;
               t_get_header_data_for_stg_tab (i).Fulf_Order_Type :=
                  v_ful_order_type;

               ------------------------------------------derive backlog_order_tie_type--------------------------------------------------------------------
               lv_error_location := 1.8;
                --Begin Added Backlog_Order_Tie_types for ReadyStock Stock/Pick --<RK004>  and <RK006>
              IF
                  t_get_header_data_for_stg_tab (i).PICK_MAKE = 'SYS-MAKE'
                  AND t_get_header_data_for_stg_tab (i).SYSTEM_TYPE_CODE = 'SYS'
                  AND t_get_header_data_for_stg_tab (i).SSC_NAME IS NULL
                  AND t_get_header_data_for_stg_tab (i).om_order_type = 'ReadyStock Stock'
              THEN
                  t_get_header_data_for_stg_tab (i).BACKLOG_ORDER_TIE_TYPE := 'CTO-SEED-ORDER';
                  t_get_header_data_for_stg_tab (i).ORDER_SOURCE := 'OMS-SEED';
                  t_get_header_data_for_stg_tab (i).FULF_ORDER_TYPE := 'SUPPLY';
              ELSIF
                  t_get_header_data_for_stg_tab (i).PICK_MAKE <> 'SYS-MAKE'
                  AND t_get_header_data_for_stg_tab (i).SYSTEM_TYPE_CODE = 'SYS'
                  AND t_get_header_data_for_stg_tab (i).SSC_NAME IS NULL
                  AND t_get_header_data_for_stg_tab (i).om_order_type = 'ReadyStock Stock'
              THEN
                  t_get_header_data_for_stg_tab (i).BACKLOG_ORDER_TIE_TYPE := 'CTO-SEED-ORDER';
                  t_get_header_data_for_stg_tab (i).ORDER_SOURCE := 'OMS-SEED';
                  t_get_header_data_for_stg_tab (i).FULF_ORDER_TYPE := 'SUPPLY';
              ELSIF
                  t_get_header_data_for_stg_tab (i).SYSTEM_TYPE_CODE = 'SYS'
                  AND t_get_header_data_for_stg_tab (i).SSC_NAME IS NULL
                  AND t_get_header_data_for_stg_tab (i).om_order_type = 'ReadyStock Stock'
              THEN
                 t_get_header_data_for_stg_tab (i).BACKLOG_ORDER_TIE_TYPE := 'CTO-SEED-ORDER';
                 t_get_header_data_for_stg_tab (i).ORDER_SOURCE := 'OMS-SEED';
                 t_get_header_data_for_stg_tab (i).FULF_ORDER_TYPE := 'SUPPLY';
              ELSIF
                  t_get_header_data_for_stg_tab (i).PICK_MAKE = 'SYS-MAKE'
                  AND t_get_header_data_for_stg_tab (i).SYSTEM_TYPE_CODE = 'SYS'
                  AND t_get_header_data_for_stg_tab (i).SSC_NAME = 'BTO'
                  AND t_get_header_data_for_stg_tab (i).om_order_type = 'ReadyStock Stock'
              THEN
                  t_get_header_data_for_stg_tab (i).BACKLOG_ORDER_TIE_TYPE := 'BTO-SEED-ORDER';
                  t_get_header_data_for_stg_tab (i).ORDER_SOURCE := 'OMS-SEED';
                  t_get_header_data_for_stg_tab (i).FULF_ORDER_TYPE := 'SUPPLY';
              ELSIF
                  t_get_header_data_for_stg_tab (i).PICK_MAKE <> 'SYS-MAKE'
                  AND t_get_header_data_for_stg_tab (i).SYSTEM_TYPE_CODE = 'SYS'
                  AND t_get_header_data_for_stg_tab (i).SSC_NAME ='BTO'
                  AND t_get_header_data_for_stg_tab (i).om_order_type = 'ReadyStock Stock'
              THEN
                  t_get_header_data_for_stg_tab (i).BACKLOG_ORDER_TIE_TYPE := 'BTO-SEED-ORDER';
                  t_get_header_data_for_stg_tab (i).ORDER_SOURCE := 'OMS-SEED';
                  t_get_header_data_for_stg_tab (i).FULF_ORDER_TYPE := 'SUPPLY';
              ELSIF
                  t_get_header_data_for_stg_tab (i).SYSTEM_TYPE_CODE = 'SYS'
                  AND t_get_header_data_for_stg_tab (i).SSC_NAME ='BTO'
                  AND t_get_header_data_for_stg_tab (i).om_order_type = 'ReadyStock Stock'
              THEN
                  t_get_header_data_for_stg_tab (i).BACKLOG_ORDER_TIE_TYPE := 'BTO-SEED-ORDER';
                  t_get_header_data_for_stg_tab (i).ORDER_SOURCE := 'OMS-SEED';
                  t_get_header_data_for_stg_tab (i).FULF_ORDER_TYPE := 'SUPPLY';
              ELSIF
                  t_get_header_data_for_stg_tab (i).om_order_type = 'ReadyStock Stock'
                  AND t_get_header_data_for_stg_tab (i).SYSTEM_TYPE_CODE = 'NON-SYS'
                  AND (t_get_header_data_for_stg_tab (i).SSC_NAME IS NULL OR t_get_header_data_for_stg_tab (i).SSC_NAME ='BTO')
              THEN
                  t_get_header_data_for_stg_tab (i).BACKLOG_ORDER_TIE_TYPE := 'SUPPLY-SEED-OPTIONS';
                  t_get_header_data_for_stg_tab (i).ORDER_SOURCE := 'OMS-SEED';
                  t_get_header_data_for_stg_tab (i).FULF_ORDER_TYPE := 'SUPPLY';
              ELSIF
                  t_get_header_data_for_stg_tab (i).PICK_MAKE = 'SYS-MAKE'
                  AND t_get_header_data_for_stg_tab (i).SYSTEM_TYPE_CODE = 'SYS'
                  AND t_get_header_data_for_stg_tab (i).SSC_NAME IS NULL
                  AND t_get_header_data_for_stg_tab (i).om_order_type = 'ReadyStock Pick'
              THEN
                  t_get_header_data_for_stg_tab (i).BACKLOG_ORDER_TIE_TYPE := 'CTO-CUST-PICK-FINISHEDGOODS';
                  t_get_header_data_for_stg_tab (i).ORDER_SOURCE := 'OMS-CUSTOMER';
                  t_get_header_data_for_stg_tab (i).FULF_ORDER_TYPE := 'CUSTOMER';
              ELSIF
                  t_get_header_data_for_stg_tab (i).PICK_MAKE <> 'SYS-MAKE'
                  AND t_get_header_data_for_stg_tab (i).SYSTEM_TYPE_CODE = 'SYS'
                  AND t_get_header_data_for_stg_tab (i).SSC_NAME IS NULL
                  AND t_get_header_data_for_stg_tab (i).om_order_type = 'ReadyStock Pick'
              THEN
                  t_get_header_data_for_stg_tab (i).BACKLOG_ORDER_TIE_TYPE := 'CTO-CUST-PICK-FINISHEDGOODS';
                  t_get_header_data_for_stg_tab (i).ORDER_SOURCE := 'OMS-CUSTOMER';
                  t_get_header_data_for_stg_tab (i).FULF_ORDER_TYPE := 'CUSTOMER';
              ELSIF
                  t_get_header_data_for_stg_tab (i).SYSTEM_TYPE_CODE = 'SYS'
                  AND t_get_header_data_for_stg_tab (i).SSC_NAME IS NULL
                  AND t_get_header_data_for_stg_tab (i).om_order_type = 'ReadyStock Pick'
              THEN
                  t_get_header_data_for_stg_tab (i).BACKLOG_ORDER_TIE_TYPE := 'CTO-CUST-PICK-FINISHEDGOODS';
                  t_get_header_data_for_stg_tab (i).ORDER_SOURCE := 'OMS-CUSTOMER';
                  t_get_header_data_for_stg_tab (i).FULF_ORDER_TYPE := 'CUSTOMER';
              ELSIF
                  t_get_header_data_for_stg_tab (i).PICK_MAKE = 'SYS-MAKE'
                  AND t_get_header_data_for_stg_tab (i).SYSTEM_TYPE_CODE = 'SYS'
                  AND t_get_header_data_for_stg_tab (i).SSC_NAME = 'BTO'
                  AND t_get_header_data_for_stg_tab (i).om_order_type = 'ReadyStock Pick'
              THEN
                  t_get_header_data_for_stg_tab (i).BACKLOG_ORDER_TIE_TYPE := 'BTO-CUST-PICK-FINISHEDGOODS';
                  t_get_header_data_for_stg_tab (i).ORDER_SOURCE := 'OMS-CUSTOMER';
                  t_get_header_data_for_stg_tab (i).FULF_ORDER_TYPE := 'CUSTOMER';
              ELSIF
                  t_get_header_data_for_stg_tab (i).PICK_MAKE <> 'SYS-MAKE'
                  AND t_get_header_data_for_stg_tab (i).SYSTEM_TYPE_CODE = 'SYS'
                  AND t_get_header_data_for_stg_tab (i).SSC_NAME = 'BTO'
                  AND t_get_header_data_for_stg_tab (i).om_order_type = 'ReadyStock Pick'
              THEN
                  t_get_header_data_for_stg_tab (i).BACKLOG_ORDER_TIE_TYPE := 'BTO-CUST-PICK-FINISHEDGOODS';
                  t_get_header_data_for_stg_tab (i).ORDER_SOURCE := 'OMS-CUSTOMER';
                  t_get_header_data_for_stg_tab (i).FULF_ORDER_TYPE := 'CUSTOMER';
              ELSIF
                  t_get_header_data_for_stg_tab (i).SYSTEM_TYPE_CODE = 'SYS'
                  AND t_get_header_data_for_stg_tab (i).SSC_NAME ='BTO'
                  AND t_get_header_data_for_stg_tab (i).om_order_type = 'ReadyStock Pick'
              THEN
                  t_get_header_data_for_stg_tab (i).BACKLOG_ORDER_TIE_TYPE := 'BTO-CUST-PICK-FINISHEDGOODS';
                  t_get_header_data_for_stg_tab (i).ORDER_SOURCE := 'OMS-CUSTOMER';
                  t_get_header_data_for_stg_tab (i).FULF_ORDER_TYPE := 'CUSTOMER';
              ELSIF
                  t_get_header_data_for_stg_tab (i).OM_ORDER_TYPE = 'ReadyStock Pick'
                  AND t_get_header_data_for_stg_tab (i).PICK_MAKE <> 'SYS-MAKE'
                  AND t_get_header_data_for_stg_tab (i).SYSTEM_TYPE_CODE = 'NON-SYS'
                  AND (t_get_header_data_for_stg_tab (i).SSC_NAME IS NULL OR t_get_header_data_for_stg_tab (i).SSC_NAME ='BTO')
              THEN
                  t_get_header_data_for_stg_tab (i).BACKLOG_ORDER_TIE_TYPE := 'CUST-PICK-OPTIONS';
                  t_get_header_data_for_stg_tab (i).ORDER_SOURCE := 'OMS-CUSTOMER';
                  t_get_header_data_for_stg_tab (i).FULF_ORDER_TYPE := 'CUSTOMER';
  --End Added Backlog_Order_Tie_types for ReadyStock Stock/Pick --<RK004>  and <RK006>
  ---Existing Backlog Order Tie Types
              ELSIF
                  t_get_header_data_for_stg_tab (i).SYSTEM_TYPE_CODE = 'SYS'
                  AND t_get_header_data_for_stg_tab (i).SSC_NAME IS NULL
                  AND t_get_header_data_for_stg_tab (i).order_source = 'OMS-SEED'
              THEN
                  t_get_header_data_for_stg_tab (i).BACKLOG_ORDER_TIE_TYPE := 'CTO-SEED-ORDER';
              ELSIF
                  t_get_header_data_for_stg_tab (i).SYSTEM_TYPE_CODE =  'SYS'
                  AND t_get_header_data_for_stg_tab (i).SSC_NAME LIKE 'BT%'
                  AND t_get_header_data_for_stg_tab (i).order_source = 'OMS-SEED'
              THEN
                   t_get_header_data_for_stg_tab (i).BACKLOG_ORDER_TIE_TYPE := 'SS-SEED-ORDER';
              ELSIF
                  t_get_header_data_for_stg_tab (i).PICK_MAKE = 'SYS-MAKE'
                  AND t_get_header_data_for_stg_tab (i).SYSTEM_TYPE_CODE = 'SYS'
                  AND t_get_header_data_for_stg_tab (i).SSC_NAME IS NULL
                  AND t_get_header_data_for_stg_tab (i).order_source <> 'OMS-SEED'
              THEN
                  t_get_header_data_for_stg_tab (i).BACKLOG_ORDER_TIE_TYPE := 'CTO-CUST-MAKE';
              ELSIF
                  t_get_header_data_for_stg_tab (i).PICK_MAKE = 'SYS-MAKE'
                  AND t_get_header_data_for_stg_tab (i).SYSTEM_TYPE_CODE = 'SYS'
                  AND t_get_header_data_for_stg_tab (i).SSC_NAME = 'BTO'
                  AND t_get_header_data_for_stg_tab (i).order_source <> 'OMS-SEED'
              THEN
                   t_get_header_data_for_stg_tab (i).BACKLOG_ORDER_TIE_TYPE := 'BTO-CUST-MAKE';
                --Changes to include FGA Direct Ship Orders Starts
              ELSIF
                  t_get_header_data_for_stg_tab (i).PICK_MAKE = 'SYS-MAKE'
                  AND t_get_header_data_for_stg_tab (i).SYSTEM_TYPE_CODE = 'SYS'
                  AND t_get_header_data_for_stg_tab (i).SSC_NAME = 'BTS'
                  AND t_get_header_data_for_stg_tab (i).order_source <> 'OMS-SEED'
                  AND t_get_header_data_for_stg_tab (i).FGA_DIRECT_SHIP_FLAG = 'Y'
              THEN
                  t_get_header_data_for_stg_tab (i).BACKLOG_ORDER_TIE_TYPE := 'BTO-CUST-MAKE';
                --Changes to include FGA Direct Ship Orders Ends
              ELSIF
                  t_get_header_data_for_stg_tab (i).PICK_MAKE <> 'SYS-MAKE'
                  AND t_get_header_data_for_stg_tab (i).SYSTEM_TYPE_CODE = 'SYS'
                  AND t_get_header_data_for_stg_tab (i).SSC_NAME IS NULL
              THEN
                  t_get_header_data_for_stg_tab (i).BACKLOG_ORDER_TIE_TYPE := 'CTO-CUST-PICK-FINISHEDGOODS';
              ELSIF
                  t_get_header_data_for_stg_tab (i).PICK_MAKE <> 'SYS-MAKE'
                  AND t_get_header_data_for_stg_tab (i).SYSTEM_TYPE_CODE = 'SYS'
                  AND t_get_header_data_for_stg_tab (i).SSC_NAME = 'BTO'
              THEN
                  t_get_header_data_for_stg_tab (i).BACKLOG_ORDER_TIE_TYPE := 'BTO-CUST-PICK-FINISHEDGOODS';
              ELSIF
                  t_get_header_data_for_stg_tab (i).PICK_MAKE <> 'SYS-MAKE'
                  AND t_get_header_data_for_stg_tab (i).SYSTEM_TYPE_CODE = 'SYS'
                  AND t_get_header_data_for_stg_tab (i).SSC_NAME IN ('BTS','BTP')
              THEN
                  t_get_header_data_for_stg_tab (i).BACKLOG_ORDER_TIE_TYPE := 'BTS-BTP-CUST-PICK-FINISHEDGOODS';
              ELSIF
                  t_get_header_data_for_stg_tab (i).PICK_MAKE <> 'SYS-MAKE'
                  AND t_get_header_data_for_stg_tab (i).SYSTEM_TYPE_CODE = 'NON-SYS'
              THEN
                  t_get_header_data_for_stg_tab (i).BACKLOG_ORDER_TIE_TYPE := 'CUST-PICK-OPTIONS';
              ELSIF
                  t_get_header_data_for_stg_tab (i).PICK_MAKE = 'SYS-MAKE'
              THEN
                  t_get_header_data_for_stg_tab (i).BACKLOG_ORDER_TIE_TYPE := 'MAKE-UNK';
              ELSIF
                  t_get_header_data_for_stg_tab (i).PICK_MAKE <> 'SYS-MAKE'
              THEN
                  t_get_header_data_for_stg_tab (i).BACKLOG_ORDER_TIE_TYPE :='PICK-UNK';                  -- 'CTO-CUST-MAKE - DEFAULT'
              ELSE
                  t_get_header_data_for_stg_tab (i).BACKLOG_ORDER_TIE_TYPE := 'UNK';
              END IF;

                dbms_output.put_line(t_get_header_data_for_stg_tab (i).order_num);
               dbms_output.put_line(t_get_header_data_for_stg_tab (i).BACKLOG_ORDER_TIE_TYPE);
               dbms_output.put_line(t_get_header_data_for_stg_tab (i).PICK_MAKE);
               dbms_output.put_line(t_get_header_data_for_stg_tab (i).SYSTEM_TYPE_CODE);
               dbms_output.put_line(t_get_header_data_for_stg_tab (i).FULFILLMENT_REGION_CODE);

             /*  If t_get_header_data_for_stg_tab (i).BACKLOG_ORDER_TIE_TYPE = 'CTO-CUST-PICK-FINISHEDGOODS'
               and t_get_header_data_for_stg_tab (i).om_order_type = 'ReadyStock Pick'
               and t_get_header_data_for_stg_tab (i).rsid is not null
               then
                  delete from scdh_reporting.om_backlog_detail
                  where order_num = t_get_header_data_for_stg_tab (i).order_num
                  and tie_number= t_get_header_data_for_stg_tab (i).tie_number;
               End If; */
               -------------------------------------calculate stage and sub stage-----------------------------------------------------------------------
               --  Pending
               lv_error_location := 1.9;
               v_case_sql :=
                  fn_get_case_stmt (
                     t_get_header_data_for_stg_tab (i).FULFILLMENT_REGION_CODE,
                     t_get_header_data_for_stg_tab (i).backlog_order_tie_type,
                     t_stg_substg_case_stmt);
                 -- dbms_output.put_line('t_stg_substg_case_stmt:'||t_stg_substg_case_stmt);
                 dbms_output.put_line('v_case_sql:'||v_case_sql);
               IF v_case_sql != 'UNK'
               THEN
                  dbms_output.put_line('INSIDE IF')     ;
                  EXECUTE IMMEDIATE v_case_sql
                     INTO v_stage_sub_stg
                     USING t_get_header_data_for_stg_tab (i).ORDER_NUM,
                           t_get_header_data_for_stg_tab (i).prod_ORDER_NUM,
                           t_get_header_data_for_stg_tab (i).tie_number,
                           t_get_header_data_for_stg_tab (i).REGION_CODE,
                           t_get_header_data_for_stg_tab (i).bu_id,
                           p_mod;
               dbms_output.put_line('v_stage_sub_stg'||v_stage_sub_stg)     ;
               ELSE
                  v_stage_sub_stg := 'UNK#UNK';
               END IF;

               t_get_header_data_for_stg_tab (i).cycle_stage_code :=
                  SUBSTR (v_stage_sub_stg,
                          1,
                          INSTR (v_stage_sub_stg, '#') - 1);
               t_get_header_data_for_stg_tab (i).cycle_substage_code :=
                  SUBSTR (v_stage_sub_stg, INSTR (v_stage_sub_stg, '#') + 1);
          dbms_output.put_line('t_get_header_data_for_stg_tab (i).cycle_substage_code:'||t_get_header_data_for_stg_tab (i).cycle_substage_code);
               ------------------------------------get backlog facility and consumption status--------------------------------------------
               lv_error_location := 2.0;

               IF t_get_header_data_for_stg_tab (i).cycle_substage_code !=
                     'UNK'
               THEN
                  v_BKLG_FCLTY_CON_STS_sql :=
                     fn_get_case_stmt (
                        t_get_header_data_for_stg_tab (i).FULFILLMENT_REGION_CODE,
                        t_get_header_data_for_stg_tab (i).backlog_order_tie_type,
                        t_backlog_CON_STS_case_stmt);

                  dbms_output.put_line('v_BKLG_FCLTY_CON_STS_sql:'||v_BKLG_FCLTY_CON_STS_sql);
                  IF v_BKLG_FCLTY_CON_STS_sql != 'UNK'
                  THEN
                 -- dbms_output.put_line('in_BKLG_FCLTY_CON_STS_sql:');
                     v_cursor := DBMS_SQL.open_cursor;
                     DBMS_SQL.parse (v_cursor,
                                     v_BKLG_FCLTY_CON_STS_sql,
                                     DBMS_SQL.native);

                     DBMS_SQL.bind_variable (
                        v_cursor,
                        ':cycle_substage_code',
                        t_get_header_data_for_stg_tab (i).cycle_substage_code);
                     DBMS_SQL.bind_variable (
                        v_cursor,
                        ':ORDER_NUM',
                        t_get_header_data_for_stg_tab (i).ORDER_NUM);
                     DBMS_SQL.bind_variable (
                        v_cursor,
                        ':prod_order_num',
                        t_get_header_data_for_stg_tab (i).prod_ORDER_NUM);
                     DBMS_SQL.bind_variable (
                        v_cursor,
                        ':tie_number',
                        t_get_header_data_for_stg_tab (i).tie_number);
                     DBMS_SQL.bind_variable (
                        v_cursor,
                        ':region_code',
                        t_get_header_data_for_stg_tab (i).REGION_CODE);
                     DBMS_SQL.bind_variable (
                        v_cursor,
                        ':bu_id',
                        t_get_header_data_for_stg_tab (i).bu_id);
                     DBMS_SQL.bind_variable (
                        v_cursor,
                        ':mod_num',
                        t_get_header_data_for_stg_tab (i).mod_num);         --

                     v_execute := DBMS_SQL.execute (v_cursor);
                     lref_cursor := DBMS_SQL.to_refcursor (v_cursor);

                     lv_error_location := 2.1;

                     FETCH lref_cursor BULK COLLECT INTO T_detail_get_pk_data;

                     CLOSE lref_cursor;

                     IF T_detail_get_pk_data.COUNT > 0
                     THEN
                        FOR p IN T_detail_get_pk_data.FIRST ..
                                 T_detail_get_pk_data.LAST
                        LOOP
                           t_detail_bklg_fclty_con_sts.EXTEND;
                           t_detail_bklg_fclty_con_sts (
                              t_detail_bklg_fclty_con_sts.LAST).region_code :=
                              t_get_header_data_for_stg_tab (i).region_code;
                           t_detail_bklg_fclty_con_sts (
                              t_detail_bklg_fclty_con_sts.LAST).bu_id :=
                              t_get_header_data_for_stg_tab (i).bu_id;
                           t_detail_bklg_fclty_con_sts (
                              t_detail_bklg_fclty_con_sts.LAST).ORDER_NUM :=
                              t_get_header_data_for_stg_tab (i).ORDER_NUM;
                           t_detail_bklg_fclty_con_sts (
                              t_detail_bklg_fclty_con_sts.LAST).prod_ORDER_NUM :=
                              t_get_header_data_for_stg_tab (i).prod_ORDER_NUM;
                           t_detail_bklg_fclty_con_sts (
                              t_detail_bklg_fclty_con_sts.LAST).DTL_SEQ_NUM :=
                              T_detail_get_pk_data (p).DTL_SEQ_NUM;
                           t_detail_bklg_fclty_con_sts (
                              t_detail_bklg_fclty_con_sts.LAST).tie_number :=
                              t_get_header_data_for_stg_tab (i).tie_number;
                           t_detail_bklg_fclty_con_sts (
                              t_detail_bklg_fclty_con_sts.LAST).mod_part_number :=
                              T_detail_get_pk_data (p).mod_part_number;
                           t_detail_bklg_fclty_con_sts (
                              t_detail_bklg_fclty_con_sts.LAST).part_number :=
                              T_detail_get_pk_data (p).part_number;
                           t_detail_bklg_fclty_con_sts (
                              t_detail_bklg_fclty_con_sts.LAST).backlog_facility :=
                              T_detail_get_pk_data (p).backlog_facility;
                           t_detail_bklg_fclty_con_sts (
                              t_detail_bklg_fclty_con_sts.LAST).consumption_status :=
                              T_detail_get_pk_data (p).consumption_status;
                           t_detail_bklg_fclty_con_sts (
                              t_detail_bklg_fclty_con_sts.LAST).VROWID :=
                              T_detail_get_pk_data (p).VROWID;
                            dbms_output.put_line('t_detail_bklg_fclty_con_sts (
                              t_detail_bklg_fclty_con_sts.LAST).consumption_status:'||t_detail_bklg_fclty_con_sts (
                              t_detail_bklg_fclty_con_sts.LAST).consumption_status);
                        END LOOP;
                     END IF;
                  END IF;
               END IF;

               IF cur_hld_detail%ISOPEN
               THEN
                  CLOSE cur_hld_detail;
               END IF;

               IF cur_fulfillment_reg%ISOPEN
               THEN
                  CLOSE cur_fulfillment_reg;
               END IF;
            EXCEPTION
               WHEN OTHERS
               THEN
                  IF cur_hld_detail%ISOPEN
                  THEN
                     CLOSE cur_hld_detail;
                  END IF;

                  IF cur_fulfillment_reg%ISOPEN
                  THEN
                     CLOSE cur_fulfillment_reg;
                  END IF;

                  lv_loop_cnt := lv_loop_cnt + 1;
                  t_excp_po (lv_loop_cnt).order_num := v_chk_order;
                  t_excp_po (lv_loop_cnt).region_code := p_region_code;
                  t_excp_po (lv_loop_cnt).sys_err_code := SQLCODE;
                  t_excp_po (lv_loop_cnt).sys_err_mesg :=
                     SQLERRM || 'Error Location:' || lv_error_location;
            END;
         END LOOP;

         lv_tot_rec_cnt :=
            lv_tot_rec_cnt + t_get_header_data_for_stg_tab.COUNT;

         IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
               P_NUMBER_OF_ROWS_SELECTED   => lv_tot_rec_cnt,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => NULL,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'OTHER FIELDS CALCULATION DONE',
               P_JOB_AUDIT_LOG_ID          => lv_job_audit_log_id_1,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);

            IF lv_error_code <> 0
            THEN
               P_ERR_CODE := lv_error_code;
               p_err_mesg :=
                  lv_error_msg || 'Error Location:' || lv_error_location;
               RAISE exp_post;
            END IF;
         END IF;

         lv_updated_rec := 0;
         lv_selcted_rec := 0;
         lv_error_location := 2.2;

         IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
               P_STEP_NAME                 => 'UPDATE',
               P_JOB_AUDIT_MESSAGE         =>    'UPDATE FOR SATGE AND SUB STAGE FOR - '
                                              || p_region_CODE,
               P_NUMBER_OF_ROWS_SELECTED   => NULL,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => NULL,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_IS_BASE_TABLE             => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           =>    'Updation started for '
                                              || t_get_header_data_for_stg_tab.COUNT
                                              || ' records',
               P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
               P_JOB_INSTANCE_ID           => lv_job_instance_id,
               P_JOB_NAME                  => lv_job_name,
               P_JOB_AUDIT_LEVEL_ID        => NULL,
               P_COMMAND_NAME              => 'SQL',
               P_COMMAND_TYPE              => 'UPDATE',
               P_JOB_AUDIT_LOG_ID          => lv_job_audit_log_id_2,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);

            IF lv_error_code <> 0
            THEN
               p_err_code := lv_error_code;
               p_err_mesg :=
                  lv_error_msg || 'Error Location:' || lv_error_location;
               RAISE exp_post;
            END IF;
         END IF;

         BEGIN
            FORALL J
                IN t_get_header_data_for_stg_tab.FIRST ..
                   t_get_header_data_for_stg_tab.LAST
              SAVE EXCEPTIONS
               UPDATE SCDH_REPORTING.OM_BACKLOG_HEADER_STG
                  SET cycle_stage_code =
                         NVL (
                            t_get_header_data_for_stg_tab (j).cycle_stage_code,
                            'UNK'),
                      cycle_substage_code =
                         NVL (
                            t_get_header_data_for_stg_tab (j).cycle_substage_code,
                            'UNK'),
                      ASN_LIFE_CYCLE_SUB_STATUS =
                         t_get_header_data_for_stg_tab (j).cycle_substage_code,
                      HOLD_CODE = t_get_header_data_for_stg_tab (j).HOLD_CODE,
                      HOLD_FLAG = t_get_header_data_for_stg_tab (j).HOLD_FLAG,
                      FULFILLMENT_REGION_CODE =
                         t_get_header_data_for_stg_tab (j).FULFILLMENT_REGION_CODE,
                      ai_doms_order_id =
                         t_get_header_data_for_stg_tab (j).ai_doms_order_id,
                      SOURCE_LOCAL_CHANNEL_CODE =
                         t_get_header_data_for_stg_tab (j).SOURCE_LOCAL_CHANNEL_CODE,
                      ORDER_SOURCE =
                         t_get_header_data_for_stg_tab (j).ORDER_SOURCE,
                      Fulf_Order_Type =
                         t_get_header_data_for_stg_tab (j).Fulf_Order_Type,
                      BACKLOG_ORDER_TIE_TYPE =
                         t_get_header_data_for_stg_tab (j).BACKLOG_ORDER_TIE_TYPE,
                      cycle_date =
                         DECODE (
                            t_get_header_data_for_stg_tab (j).cycle_substage_code,
                            'VOI', SHIP_NOTIFICATION_DATE,
                            'DOI', SHIP_NOTIFICATION_DATE,
                            CYCLE_DATE)
                WHERE ROWID = t_get_header_data_for_stg_tab (j).ROWID;

            lv_updated_rec := lv_updated_rec + SQL%ROWCOUNT;
            lv_selcted_rec :=
               lv_selcted_rec + t_get_header_data_for_stg_tab.COUNT;
            COMMIT;
         EXCEPTION
            WHEN OTHERS
            THEN
               FOR i IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
               LOOP
                  IF t_get_header_data_for_stg_tab.EXISTS (
                        SQL%BULK_EXCEPTIONS (i).ERROR_INDEX)
                  THEN
                     lv_indx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
                     lv_loop_cnt := lv_loop_cnt + 1;
                     t_excp_po (lv_loop_cnt).prod_order_num :=
                        t_get_header_data_for_stg_tab (lv_indx).Prod_order_NUM;
                     t_excp_po (lv_loop_cnt).order_num :=
                        t_get_header_data_for_stg_tab (lv_indx).order_num;
                     t_excp_po (lv_loop_cnt).region_code :=
                        t_get_header_data_for_stg_tab (lv_indx).region_code;
                     t_excp_po (lv_loop_cnt).tie_number :=
                        t_get_header_data_for_stg_tab (lv_indx).tie_number;
                     t_excp_po (lv_loop_cnt).bu_id :=
                        t_get_header_data_for_stg_tab (lv_indx).bu_id;
                     t_excp_po (lv_loop_cnt).sys_err_code :=
                        SQL%BULK_EXCEPTIONS (i).ERROR_CODE;
                     t_excp_po (lv_loop_cnt).sys_err_mesg :=
                           SQLERRM(SQL%BULK_EXCEPTIONS (i).ERROR_CODE * -1)
                        || 'ERR LOC '
                        || lv_error_location;
                  END IF;
               END LOOP;
         END;

         IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
               P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => lv_updated_rec,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'UPDATION DONE FOR OM BACKLOG HEADER STG AND SUB STG',
               P_JOB_AUDIT_LOG_ID          => lv_job_audit_log_id_2,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);

            IF lv_error_code <> 0
            THEN
               P_ERR_CODE := lv_error_code;
               p_err_mesg :=
                  lv_error_msg || 'Error Location:' || lv_error_location;
               RAISE exp_post;
            END IF;
         END IF;

         lv_updated_rec := 0;
         lv_selcted_rec := 0;
         lv_error_location := 2.3;

         IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
               P_STEP_NAME                 => 'UPDATE',
               P_JOB_AUDIT_MESSAGE         =>    'UPDATE FOR BKLOG FACILITY AND CONSUMPTION STATUS FOR - '
                                              || p_region_CODE,
               P_NUMBER_OF_ROWS_SELECTED   => NULL,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => NULL,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_IS_BASE_TABLE             => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           =>    'Updation started for '
                                              || t_detail_bklg_fclty_con_sts.COUNT
                                              || ' records',
               P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
               P_JOB_INSTANCE_ID           => lv_job_instance_id,
               P_JOB_NAME                  => lv_job_name,
               P_JOB_AUDIT_LEVEL_ID        => NULL,
               P_COMMAND_NAME              => 'SQL',
               P_COMMAND_TYPE              => 'UPDATE',
               P_JOB_AUDIT_LOG_ID          => lv_job_audit_log_id_2,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);

            IF lv_error_code <> 0
            THEN
               p_err_code := lv_error_code;
               p_err_mesg :=
                  lv_error_msg || 'Error Location:' || lv_error_location;
               RAISE exp_post;
            END IF;
         END IF;

         BEGIN
            FORALL h
                IN t_detail_bklg_fclty_con_sts.FIRST ..
                   t_detail_bklg_fclty_con_sts.LAST
               UPDATE SCDH_REPORTING.OM_BACKLOG_DETAIL_STG
                  SET backlog_facility =
                         NVL (
                            t_detail_bklg_fclty_con_sts (h).backlog_facility,
                            CONSUMPTION_FACILITY),
                      consumption_status =
                         NVL (
                            t_detail_bklg_fclty_con_sts (h).consumption_status,
                            'UNCONSUMED')
                WHERE ROWID = t_detail_bklg_fclty_con_sts (h).VROWID;

            lv_updated_rec := lv_updated_rec + SQL%ROWCOUNT;
            lv_selcted_rec :=
               lv_selcted_rec + t_detail_bklg_fclty_con_sts.COUNT;
            COMMIT;
         EXCEPTION
            WHEN OTHERS
            THEN
               FOR i IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
               LOOP
                  IF t_detail_bklg_fclty_con_sts.EXISTS (
                        SQL%BULK_EXCEPTIONS (i).ERROR_INDEX)
                  THEN
                     lv_indx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
                     lv_loop_cnt := lv_loop_cnt + 1;
                     t_excp_po (lv_loop_cnt).prod_order_num :=
                        t_detail_bklg_fclty_con_sts (lv_indx).Prod_order_NUM;
                     t_excp_po (lv_loop_cnt).order_num :=
                        t_detail_bklg_fclty_con_sts (lv_indx).order_num;
                     t_excp_po (lv_loop_cnt).region_code :=
                        t_detail_bklg_fclty_con_sts (lv_indx).region_code;
                     t_excp_po (lv_loop_cnt).tie_number :=
                        t_detail_bklg_fclty_con_sts (lv_indx).tie_number;
                     t_excp_po (lv_loop_cnt).bu_id :=
                        t_detail_bklg_fclty_con_sts (lv_indx).bu_id;
                     t_excp_po (lv_loop_cnt).PART_NUMBER :=
                        t_detail_bklg_fclty_con_sts (lv_indx).PART_NUMBER;
                     t_excp_po (lv_loop_cnt).MOD_PART_NUMBER :=
                        t_detail_bklg_fclty_con_sts (lv_indx).MOD_PART_NUMBER;
                     t_excp_po (lv_loop_cnt).DTL_SEQ_NUM :=
                        t_detail_bklg_fclty_con_sts (lv_indx).DTL_SEQ_NUM;
                     t_excp_po (lv_loop_cnt).sys_err_code :=
                        SQL%BULK_EXCEPTIONS (i).ERROR_CODE;
                     t_excp_po (lv_loop_cnt).sys_err_mesg :=
                           SQLERRM (SQL%BULK_EXCEPTIONS (i).ERROR_CODE * -1)
                        || 'ERR LOC '
                        || lv_error_location;
                  END IF;
               END LOOP;
         END;

         IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
               P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => lv_updated_rec,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'UPDATION DONE FOR OM BACKLOG DETAIL BKLOG FACILITY AND CONSUMPTION STATUS',
               P_JOB_AUDIT_LOG_ID          => lv_job_audit_log_id_2,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);

            IF lv_error_code <> 0
            THEN
               P_ERR_CODE := lv_error_code;
               p_err_mesg :=
                  lv_error_msg || 'Error Location:' || lv_error_location;
               RAISE exp_post;
            END IF;
         END IF;
      END LOOP;

      COMMIT;

      CLOSE cur_get_header_data_for_stg;
   EXCEPTION
      WHEN exp_post
      THEN
         ROLLBACK;

         IF cur_get_header_data_for_stg%ISOPEN
         THEN
            CLOSE cur_get_header_data_for_stg;
         END IF;

         IF cur_hld_detail%ISOPEN
         THEN
            CLOSE cur_hld_detail;
         END IF;

         IF cur_fulfillment_reg%ISOPEN
         THEN
            CLOSE cur_fulfillment_reg;
         END IF;

         p_err_code := 1;
         p_err_mesg :=
               SUBSTR (SQLERRM, 1, 3500)
            || ' Err Location:'
            || lv_error_location; --||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE() ;
        dbms_output.put_line('p_err_mesg in cal_stage:'|| p_err_mesg);
      WHEN OTHERS
      THEN
         ROLLBACK;

         IF cur_get_header_data_for_stg%ISOPEN
         THEN
            CLOSE cur_get_header_data_for_stg;
         END IF;

         IF cur_hld_detail%ISOPEN
         THEN
            CLOSE cur_hld_detail;
         END IF;

         IF cur_fulfillment_reg%ISOPEN
         THEN
            CLOSE cur_fulfillment_reg;
         END IF;

         p_err_code := 1;
         p_err_mesg :=
               SUBSTR (SQLERRM, 1, 3500)
            || ' Err Location:'
            || lv_error_location;
   END;

   FUNCTION fn_get_order_source (
      p_buid_in      IN SCDH_REPORTING.CONFIG_ORDER_SOURCE.BUID%TYPE,
      p_co_name_in   IN SCDH_REPORTING.CONFIG_ORDER_SOURCE.BUID%TYPE)
      RETURN VARCHAR2
   AS
      v_ord_source   scdh_reporting.config_order_source.ORDER_SOURCE%TYPE;
   BEGIN
      SELECT NVL (COS.ORDER_SOURCE, 'OMS-CUSTOMER')
        INTO v_ord_source
        FROM scdh_reporting.config_order_source COS,
             TABLE (COS.COMPANY_NUM) TBL1
       WHERE     COS.BUID = TO_CHAR (p_buid_in)
             AND TBL1.COLUMN_VALUE = p_co_name_in;

      RETURN v_ord_source;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN 'UNK';
   END;

   FUNCTION fn_get_pickup_flag (
      p_ship_code    scdh_fulfillment.backlog_header.ship_code%TYPE)
      RETURN VARCHAR2
   AS
      v_pickup_flag   VARCHAR2 (5);
   BEGIN
      SELECT CASE WHEN SM.SHIP_METHOD_CODE = 'CP' THEN 'Y' ELSE 'N' END
                customer_pickup_flag
        INTO v_pickup_flag
        FROM scdh_master.ship_code sc
             JOIN scdh_master.ship_method sm
                ON (sc.ship_method_code = sm.ship_method_code)
       WHERE     sc.SYS_ENT_STATE = 'ACTIVE'
             AND sM.SYS_ENT_STATE = 'ACTIVE'
             AND SC.SHIP_CODE = p_ship_code
             AND SHIP_METHOD_NAME = 'CUSTOMER PICKUP';

      RETURN v_pickup_flag;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN 'N';
      WHEN OTHERS
      THEN
         IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
               P_STEP_NAME                 => 'SELECT',
               P_JOB_AUDIT_MESSAGE         =>    'CALCULATION FOR CUSTOMER PICK UP FLAG FOR p_ship_code- '
                                              || p_ship_code,
               P_NUMBER_OF_ROWS_SELECTED   => NULL,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => NULL,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_IS_BASE_TABLE             => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'Calculation started..',
               P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
               P_JOB_INSTANCE_ID           => lv_job_instance_id,
               P_JOB_NAME                  => lv_job_name,
               P_JOB_AUDIT_LEVEL_ID        => NULL,
               P_COMMAND_NAME              => 'SQL',
               P_COMMAND_TYPE              => 'SELECT',
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);

            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
               P_NUMBER_OF_ROWS_SELECTED   => NULL,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => NULL,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           =>    'CUSTOMER PICK UP FAILED FOR p_ship_code '
                                              || p_ship_code,
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);
         END IF;

         RETURN 'N';
   END;

   PROCEDURE prc_populating_header_dtl (
      p_region            IN     SCDH_FULFILLMENT.BACKLOG_HEADER.REGION_CODE%TYPE,
      p_start             IN     TIMESTAMP WITH LOCAL TIME ZONE,
      p_end               IN     TIMESTAMP WITH LOCAL TIME ZONE,
      p_mod               IN     SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE, --
      p_job_instance_id   IN     fdl_snop_scdhub.process_job_header.job_instance_id%TYPE,
      p_audit_log_yn      IN     CHAR,
      P_INIT_FLAG         IN     CHAR,
      P_insert_rec           OUT NUMBER,
      p_err_code             OUT VARCHAR2,
      p_err_mesg             OUT VARCHAR2)
   IS
      -- Cursor for Initial Data Load if needs to re-process from scratch
      CURSOR cur_get_order_init (
         p_region_in   IN SCDH_FULFILLMENT.BACKLOG_HEADER.REGION_CODE%TYPE,
         p_mod_in      IN SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE) --
      IS
         SELECT DISTINCT BH.ORDER_NUMBER ORDER_NUMBER
           FROM SCDH_FULFILLMENT.BACKLOG_HEADER BH
          WHERE     BH.REGION_CODE = p_region_in -----Assumption:Taking into consideration that an order cannot span in two different regions
                AND BH.RPT_DOMS_STATUS <> 'OPR'
                AND MOD (bh.ORDER_NUMBER, 10) = p_mod_in;                   --

      CURSOR cur_get_order (
         p_region_in       IN SCDH_FULFILLMENT.BACKLOG_HEADER.REGION_CODE%TYPE,
         p_start_date_in   IN TIMESTAMP WITH LOCAL TIME ZONE,
         p_end_date_in     IN TIMESTAMP WITH LOCAL TIME ZONE,
         p_OMS_SOURCE1     IN VARCHAR2,
         p_OMS_SOURCE2     IN VARCHAR2,
         p_mod_in          IN SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE) --
      IS
         SELECT DISTINCT ORDER_NUMBER
           FROM (WITH backlog_orders
                      AS (SELECT DISTINCT
                                 BH.ORDER_NUMBER ORDER_NUMBER,
                                 PROD_ORDER_NUMBER,
                                 BUID,
                                 REGION_CODE,
                                 BH.RPT_DOMS_STATUS,
                                 BH.SYS_LAST_MODIFIED_DATE
                                    SYS_LAST_MODIFIED_DATE_BH
                            FROM SCDH_FULFILLMENT.BACKLOG_HEADER BH
                           WHERE     BH.REGION_CODE = p_region_in -----Assumption:Taking into consideration that an order cannot span in two different regions
                                 AND BH.RPT_DOMS_STATUS <> 'OPR'
                                 AND MOD (bh.ORDER_NUMBER, 10) = p_mod_in
                                                                         )
                 SELECT bo.ORDER_NUMBER
                   FROM backlog_orders bo
                  WHERE bo.SYS_LAST_MODIFIED_DATE_BH BETWEEN p_start_date_in AND p_end_date_in
                 UNION ALL
                 SELECT /*+ use_nl(backlog_orders po) */
                        SALES_ORDER_ID AS ORDER_NUMBER
                   FROM SCDH_FULFILLMENT.PROD_ORDER PO
                        INNER JOIN backlog_orders
                           ON     backlog_orders.PROD_ORDER_NUMBER =
                                     PO.PROD_ORDER_NUM
                              AND backlog_orders.REGION_CODE = PO.REGION_CODE
                  WHERE     PO.REGION_CODE = p_region_in
                        AND SYS_EXTRACT_UTC (PO.SYS_LAST_MODIFIED_DATE) BETWEEN p_start_date_in
                                                                            AND p_end_date_in
                 UNION ALL
                 SELECT BD.ORDER_NUMBER
                   FROM SCDH_FULFILLMENT.BACKLOG_DETAIL BD
                        INNER JOIN backlog_orders
                           ON     backlog_orders.ORDER_NUMBER =
                                     BD.ORDER_NUMBER
                              AND backlog_orders.BUID = BD.BUID
                              AND backlog_orders.REGION_CODE = BD.REGION_CODE
                  WHERE     BD.REGION_CODE = p_region_in
                        AND BD.SYS_LAST_MODIFIED_DATE BETWEEN p_start_date_in
                                                          AND p_end_date_in
                 UNION ALL
                 SELECT ORDER_NUM AS ORDER_NUMBER
                   FROM SCDH_FULFILLMENT.OMS_ORDHDR
                        INNER JOIN backlog_orders
                           ON     backlog_orders.ORDER_NUMBER =
                                     OMS_ORDHDR.ORDER_NUM
                              AND backlog_orders.buid = OMS_ORDHDR.bu_id
                  WHERE     OMS_ORDHDR.OMS_SOURCE IN (p_OMS_SOURCE1,
                                                      p_OMS_SOURCE2)
                        AND SYS_EXTRACT_UTC (
                               OMS_ORDHDR.SYS_LAST_MODIFIED_DATE) BETWEEN p_start_date_in
                                                                      AND p_end_date_in
                 UNION ALL
                 SELECT WO.ORDER_NUM AS ORDER_NUMBER
                   FROM BACKLOG_ORDERS
                        INNER JOIN SCDH_FULFILLMENT.WORK_ORDER WO
                           ON     BACKLOG_ORDERS.ORDER_NUMBER = WO.ORDER_NUM
                              AND BACKLOG_ORDERS.BUID = WO.BU_ID
                        INNER JOIN SCDH_FULFILLMENT.WO_MANIFEST_XREF WMF
                           ON (    WO.WO_ID = WMF.WO_ID
                               AND WO.REGION_CODE = WMF.REGION_CODE)
                        INNER JOIN SCDH_FULFILLMENT.MANIFEST MF
                           ON (    WMF.REGION_CODE = MF.REGION_CODE
                               AND WMF.MANIFEST_REF = MF.MANIFEST_REF)
                        /* Added as part of Merge Backlog requirement*/
                        LEFT OUTER JOIN SCDH_REPORTING.asn_notification AN
                           ON (    AN.ASN_NUMBER = MF.SOURCE_MANIFEST_ID
                               AND MF.REGION_CODE = AN.REGION)
                  /* Added as part of GL BTP Orders requirement*/
                  /*    LEFT OUTER JOIN SCDH_FULFILLMENT.asn_notification_AE ANAE
                              ON (ANAE.ASN_NUMBER=MF.SOURCE_MANIFEST_ID
                                  AND MF.REGION_CODE=ANAE.REGION)*/
                  WHERE     WO.REGION_CODE = p_region_in
                        AND (   (mf.SYS_LAST_MODIFIED_DATE BETWEEN p_start_date_in
                                                               AND p_end_date_in)
                             OR (AN.SYS_LAST_MODIFIED_DATE BETWEEN p_start_date_in
                                                               AND p_end_date_in))
                 UNION ALL
                 SELECT /*+ ordered use_NL(SO BH) INDEX(SO SALES_ORDER_PK) */
                        DISTINCT so.SALES_ORDER_ID AS ORDER_NUMBER
                   FROM BACKLOG_ORDERS BH
                        INNER JOIN SCDH_FULFILLMENT.sales_order so
                           ON (    bh.order_number = so.sales_order_id
                               AND bh.buid = SO.BUID
                               AND bh.region_code = so.region_code)
                        LEFT OUTER JOIN SCDH_FULFILLMENT.SO_ATTRIBUTE SA
                           ON (    SA.SALES_ORDER_REF = SO.SALES_ORDER_REF
                               AND SA.ATTRIBUTE_NAME IN (SELECT PROPVALUE
                                                           FROM FDL_SNOP_SCDHUB.MST_SYS_PROPS
                                                          WHERE ID =
                                                                   'SO_ATTRIBUTE')
                               AND SA.ATTRIBUTE_VALUE = 'Y')
                  WHERE     SO.REGION_CODE = p_region_in
                        AND SO.SYS_LAST_MODIFIED_DATE BETWEEN p_start_date_in
                                                          AND p_end_date_in
                 UNION ALL
                 /* Added as part of Merge Backlog requirement*/
                 SELECT ORDER_NUM AS ORDER_NUMBER
                   FROM backlog_orders BH
                        INNER JOIN SCDH_REPORTING.ORDER_SHIPMENT_STATUS OSS
                           ON     bh.order_number = OSS.ORDER_NUM
                              AND bh.region_code = oss.region
                  WHERE     OSS.REGION = p_region_in
                        AND 'EMEA' = p_region_in
                        AND OSS.SYS_LAST_MODIFIED_DATE BETWEEN p_start_date_in
                                                           AND p_end_date_in
                 UNION ALL
                 /* Added as part of  PPB3 to identify order with Velocity and precision*/
                 SELECT DISTINCT bh.ORDER_NUMBER AS ORDER_NUMBER
                   FROM backlog_orders BH
                        INNER JOIN SCDH_FULFILLMENT.MFG_ORD_BKLG_VP BVP
                           ON     bh.order_number = BVP.ORD_NUM
                              AND bh.buid = BVP.BU_ID
                              AND bh.region_code = BVP.region
                              AND BVP.REGION = p_region_in
                              AND BVP.SYS_LAST_MODIFIED_DATE BETWEEN p_start_date_in
                                                                 AND p_end_date_in
                         INNER JOIN
                                 SCDH_REPORTING.OM_BACKLOG_HEADER OMH
                                 ON OMH.ORDER_NUM=BVP.ORD_NUM
                                         AND OMH.BU_ID = BVP.BU_ID
                                         AND OMH.REGION_CODE = BVP.REGION
                                         AND trunc(OMH.TIE_NUMBER)=trunc(BVP.ORD_TIE_NUM)
                                         AND TRIM(V_P_FLAG)<>TRIM(VEL_PRECISON_FLAG)
                                         AND JOB_TIME='HOURLY'
                                         AND BVP.REGION = p_region_in
                 UNION ALL
                 /* Added as part of  PPB3 to identify order from the lead time table*/
                 SELECT DISTINCT bh.ORDER_NUMBER AS ORDER_NUMBER
                   FROM backlog_orders BH
                        INNER JOIN SCDH_FULFILLMENT.SO_LEAD_TIME SOLT
                           ON     bh.order_number = SOLT.SO_NBR
                              AND bh.buid = SOLT.BU_ID
                              AND SOLT.VER_NUM=1
                              AND bh.region_code = SOLT.DELL_RGN_CD
                              AND SOLT.DELL_RGN_CD = p_region_in
                              AND SOLT.SYS_LAST_MODIFIED_DATE BETWEEN p_start_date_in
                                                                  AND p_end_date_in);

      TYPE coll_cur_get_order IS TABLE OF cur_get_order%ROWTYPE;

      t_cur_get_order             coll_cur_get_order;


      CURSOR cur_get_order_apj (
         p_region_in       IN SCDH_FULFILLMENT.BACKLOG_HEADER.REGION_CODE%TYPE,
         p_start_date_in   IN TIMESTAMP WITH LOCAL TIME ZONE,
         p_end_date_in     IN TIMESTAMP WITH LOCAL TIME ZONE,
         p_OMS_SOURCE1     IN VARCHAR2,
         p_OMS_SOURCE2     IN VARCHAR2,
         p_mod_in          IN SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE) --
      IS
         SELECT DISTINCT ORDER_NUMBER
           FROM (WITH backlog_orders
                      AS (SELECT DISTINCT
                                 BH.ORDER_NUMBER ORDER_NUMBER,
                                 PROD_ORDER_NUMBER,
                                 BUID,
                                 REGION_CODE,
                                 BH.RPT_DOMS_STATUS,
                                 BH.SYS_LAST_MODIFIED_DATE
                                    SYS_LAST_MODIFIED_DATE_BH
                            FROM SCDH_FULFILLMENT.BACKLOG_HEADER BH
                           WHERE     BH.REGION_CODE = p_region_in -----Assumption:Taking into consideration that an order cannot span in two different regions
                                 AND BH.RPT_DOMS_STATUS <> 'OPR'
                                 AND MOD (bh.ORDER_NUMBER, 10) = p_mod_in
                                                                         )
                 SELECT bo.ORDER_NUMBER
                   FROM backlog_orders bo
                  WHERE bo.SYS_LAST_MODIFIED_DATE_BH BETWEEN p_start_date_in
                                                         AND p_end_date_in
                 UNION ALL
                 SELECT /*+ use_nl(backlog_orders po) */
                        SALES_ORDER_ID AS ORDER_NUMBER
                   FROM SCDH_FULFILLMENT.PROD_ORDER PO
                        INNER JOIN backlog_orders
                           ON     backlog_orders.PROD_ORDER_NUMBER =
                                     PO.PROD_ORDER_NUM
                              AND backlog_orders.REGION_CODE = PO.REGION_CODE
                  WHERE     PO.REGION_CODE = p_region_in
                        AND SYS_EXTRACT_UTC (PO.SYS_LAST_MODIFIED_DATE) BETWEEN p_start_date_in
                                                                            AND p_end_date_in
                 UNION ALL
                 SELECT BD.ORDER_NUMBER
                   FROM SCDH_FULFILLMENT.BACKLOG_DETAIL BD
                        INNER JOIN backlog_orders
                           ON     backlog_orders.ORDER_NUMBER =
                                     BD.ORDER_NUMBER
                              AND backlog_orders.BUID = BD.BUID
                              AND backlog_orders.REGION_CODE = BD.REGION_CODE
                  WHERE     BD.REGION_CODE = p_region_in
                        AND BD.SYS_LAST_MODIFIED_DATE BETWEEN p_start_date_in
                                                          AND p_end_date_in
                 UNION ALL
                 SELECT ORDER_NUM AS ORDER_NUMBER
                   FROM SCDH_FULFILLMENT.OMS_ORDHDR
                        INNER JOIN backlog_orders
                           ON     backlog_orders.ORDER_NUMBER =
                                     OMS_ORDHDR.ORDER_NUM
                              AND backlog_orders.buid = OMS_ORDHDR.bu_id
                  WHERE     OMS_ORDHDR.OMS_SOURCE IN (p_OMS_SOURCE1,
                                                      p_OMS_SOURCE2)
                        AND SYS_EXTRACT_UTC (
                               OMS_ORDHDR.SYS_LAST_MODIFIED_DATE) BETWEEN p_start_date_in
                                                                      AND p_end_date_in
                 UNION ALL
                 SELECT WO.ORDER_NUM AS ORDER_NUMBER
                   FROM BACKLOG_ORDERS
                        INNER JOIN SCDH_FULFILLMENT.WORK_ORDER WO
                           ON     BACKLOG_ORDERS.ORDER_NUMBER = WO.ORDER_NUM
                              AND BACKLOG_ORDERS.BUID = WO.BU_ID
                        INNER JOIN SCDH_FULFILLMENT.WO_MANIFEST_XREF WMF
                           ON (    WO.WO_ID = WMF.WO_ID
                               AND WO.REGION_CODE = WMF.REGION_CODE)
                        INNER JOIN SCDH_FULFILLMENT.MANIFEST MF
                           ON (    WMF.REGION_CODE = MF.REGION_CODE
                               AND WMF.MANIFEST_REF = MF.MANIFEST_REF)
                        /* Added as part of GL BTP Orders requirement*/
                        ---- OTM_APJ changes(same as GLBTP)
                        LEFT OUTER JOIN SCDH_REPORTING.asn_notification ANAE
                           ON (    ANAE.ASN_NUMBER = MF.SOURCE_MANIFEST_ID
                               AND MF.REGION_CODE = ANAE.REGION)
                  WHERE     WO.REGION_CODE = p_region_in
                        AND mf.SYS_LAST_MODIFIED_DATE BETWEEN p_start_date_in
                                                          AND p_end_date_in
                 UNION ALL
                 SELECT so.SALES_ORDER_ID AS ORDER_NUMBER
                   FROM SCDH_FULFILLMENT.sales_order so
                        INNER JOIN backlog_orders bo
                           ON (    bo.order_number = so.sales_order_id
                               AND bo.buid =
                                      (SELECT DECODE (
                                                 v_ibu_id_flag,
                                                 'Y', NVL (
                                                         (SELECT DISTINCT
                                                                 BU_ID
                                                            FROM Scdh_master.MST_IBU_BUID_XREF
                                                           WHERE IBU_ID =
                                                                    SO.BUID),
                                                         so.buid),
                                                 SO.BUID)
                                         FROM DUAL)
                               AND bo.region_code = so.region_code)
                        LEFT OUTER JOIN SCDH_FULFILLMENT.SO_ATTRIBUTE SA
                           ON (    SA.SALES_ORDER_REF = SO.SALES_ORDER_REF
                               AND SA.ATTRIBUTE_NAME IN (SELECT PROPVALUE
                                                           FROM FDL_SNOP_SCDHUB.MST_SYS_PROPS
                                                          WHERE ID IN ('SO_ATTRIBUTE',
                                                                       'IS_OTM_ENABLED')) ----OTM_APJ Changes
                               AND SA.ATTRIBUTE_VALUE = 'Y')
                  WHERE     SO.REGION_CODE = p_region_in
                        AND SO.SYS_LAST_MODIFIED_DATE BETWEEN p_start_date_in
                                                          AND p_end_date_in
                 --           --OTM_APJ changes starts(commentedd as it is not needed, Included in previous union)
                 --           UNION ALL
                 --           SELECT so.SALES_ORDER_ID AS ORDER_NUMBER
                 --           FROM SCDH_FULFILLMENT.sales_order so
                 --                INNER JOIN backlog_orders bo
                 --                ON (bo.order_number=so.sales_order_id
                 --                and bo.buid=(select decode(v_ibu_id_flag,'Y',nvl((SELECT DISTINCT BU_ID FROM Scdh_master.MST_IBU_BUID_XREF
                 --                                            WHERE IBU_ID=SO.BUID),so.buid),SO.BUID) from dual)
                 --                and bo.region_code=so.region_code)
                 --                LEFT OUTER JOIN SCDH_FULFILLMENT.SO_ATTRIBUTE SA
                 --                ON (SA.SALES_ORDER_REF = SO.SALES_ORDER_REF
                 --                    AND SA.ATTRIBUTE_NAME IN (SELECT PROPVALUE FROM FDL_SNOP_SCDHUB.MST_SYS_PROPS WHERE ID ='IS_OTM_ENABLED')
                 --                    AND SA.ATTRIBUTE_VALUE = 'Y')
                 --          WHERE SO.REGION_CODE=p_region_in AND SO.SYS_LAST_MODIFIED_DATE BETWEEN p_start_date_in
                 --                                                 AND p_end_date_in
                 --OTM_APJ changes ends
                 /* Added as part of Merge Backlog requirement*/
                 UNION ALL
                 SELECT ORDER_NUM AS ORDER_NUMBER
                   FROM backlog_orders BH
                        INNER JOIN SCDH_REPORTING.STG_DRGN_TNT SDT
                           ON     bh.order_number = SDT.ORDER_NUM
                              AND BH.REGION_CODE = SDT.REGION
                  WHERE     SDT.REGION = p_region_in
                        AND SDT.SYS_LAST_MODIFIED_DATE BETWEEN p_start_date_in
                                                           AND p_end_date_in
                 UNION ALL
                 /* Added as part of  PPB3 to identify order with Velocity and precision*/
                 SELECT DISTINCT bh.ORDER_NUMBER AS ORDER_NUMBER
                   FROM backlog_orders BH
                        INNER JOIN SCDH_FULFILLMENT.MFG_ORD_BKLG_VP BVP
                           ON     bh.order_number = BVP.ORD_NUM
                              AND bh.buid = BVP.BU_ID
                              AND bh.region_code = BVP.region
                              AND BVP.REGION = p_region_in
                              AND BH.RPT_DOMS_STATUS='IP'
                              AND BVP.SYS_LAST_MODIFIED_DATE BETWEEN p_start_date_in
                                                                 AND p_end_date_in
                        INNER JOIN
                                 SCDH_REPORTING.OM_BACKLOG_HEADER OMH
                                 ON OMH.ORDER_NUM=BVP.ORD_NUM
                                         AND OMH.BU_ID = BVP.BU_ID
                                         AND OMH.REGION_CODE = BVP.REGION
                                         AND trunc(OMH.TIE_NUMBER)=trunc(BVP.ORD_TIE_NUM)
                                         AND V_P_FLAG<>VEL_PRECISON_FLAG
                                         AND JOB_TIME='HOURLY'
                                         AND BVP.REGION = p_region_in
                 UNION ALL
                 /* Added as part of  PPB3 to identify order from the lead time table*/
                 SELECT DISTINCT bh.ORDER_NUMBER AS ORDER_NUMBER
                   FROM backlog_orders BH
                        INNER JOIN SCDH_FULFILLMENT.SO_LEAD_TIME SOLT
                           ON     bh.order_number = SOLT.SO_NBR
                              AND bh.buid = SOLT.BU_ID
                               AND SOLT.VER_NUM=1
                              AND bh.region_code = SOLT.DELL_RGN_CD
                              AND SOLT.DELL_RGN_CD = p_region_in
                              AND SOLT.SYS_LAST_MODIFIED_DATE BETWEEN p_start_date_in
                                                                  AND p_end_date_in);



      TYPE coll_cur_get_order_apj IS TABLE OF cur_get_order_apj%ROWTYPE;

      t_cur_get_order_apj         coll_cur_get_order_apj;


      CURSOR CUR_SYS_MAKE_UNK (
         p_region    scdh_REPORTING.OM_BACKLOG_HEADER_STG.region_code%TYPE,
         p_mod       SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE)
      IS
         SELECT S.ROWID,
                PROD_ORDER_NUM,
                TIE_NUMBER,
                ORDER_NUM,
                BU_ID,
                REGION_CODE,
                JOB_TIME,
                CASE
                   WHEN     SYSTEM_TYPE_CODE = 'SYS'
                        AND EXISTS
                               (SELECT 1
                                  FROM SCDH_FULFILLMENT.LINE_SKU LS
                                       JOIN SCDH_FULFILLMENT.PART P
                                          ON     P.REGION_CODE =
                                                    LS.REGION_CODE
                                             AND P.LINE_SKU_SEQ =
                                                    LS.LINE_SKU_SEQ
                                       JOIN scdh_master.facility f
                                          ON P.CONSUMPTION_FACILITY =
                                                f.facility_code
                                 WHERE     LS.REGION_CODE = S.REGION_CODE
                                       AND LS.PROD_ORDER_NUM =
                                              S.PROD_ORDER_NUM
                                       AND LS.LINE_NUM = S.PROD_ORDER_LINE
                                       AND ff_type_name = 'MFG')
                   THEN
                      'SYS-MAKE'
                   ELSE
                      'PICK'
                END
                   AS pick_make
           FROM SCDH_REPORTING.OM_BACKLOG_HEADER_STG S
          WHERE     S.PICK_MAKE = 'UNK'
                AND REGION_CODE = p_region
                AND MOD_NUM = p_mod;                                        --

      -----Date:27-Apr-14 Comment:This cursor was put due to base_prod_code,BASE_PROD_PROT_ID,FMLY_PARNT_CODE coming as NULL.
      CURSOR CUR_PROT_ID_NULL (
         p_region    scdh_REPORTING.OM_BACKLOG_HEADER_STG.region_code%TYPE,
         p_mod       SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE)
      IS
         SELECT DISTINCT BH.ROWID,
                         BH.REGION_CODE,
                         BH.ORDER_NUM,
                         BH.PROD_ORDER_NUM,
                         BH.BU_ID,
                         BH.TIE_NUMBER,
                         v.base_prod_code,
                         v.BASE_PROD_PROT_ID,
                         v.FMLY_PARNT_CODE
           FROM SCDH_REPORTING.OM_BACKLOG_HEADER_STG BH
                JOIN SCDH_REPORTING.OM_BACKLOG_DETAIL_STG BD
                   ON     BH.REGION_CODE = BD.REGION_CODE
                      AND BH.BU_ID = BD.BU_ID
                      AND BH.ORDER_NUM = BD.ORDER_NUM
                      AND BH.PROD_ORDER_NUM = BD.PROD_ORDER_NUM
                      AND BH.TIE_NUMBER = BD.TIE_NUMBER
                      AND BH.JOB_TIME = BD.JOB_TIME
                      AND BH.MOD_NUM = BD.MOD_NUM                           --
                JOIN SCDH_OUTBOUND.dell_cm_fga_product_flat_vw v
                   ON BD.fga_id = v.fga_id
          WHERE     BH.REGION_CODE = p_region
                AND BH.MOD_NUM = p_mod                                      --
                AND BH.SYSTEM_TYPE_CODE = 'SYS'
                AND BH.SSC_NAME IN ('BTS', 'BTP', 'BTO')
                AND BH.BASE_PROD_CODE IS NULL
                AND BD.FGA_ID IS NOT NULL;

      CURSOR CUR_ITEM_LOC (
         p_region    scdh_REPORTING.OM_BACKLOG_HEADER_STG.region_code%TYPE,
         p_mod       SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE)
      IS
         SELECT IL.return_on_asn return_on_asn,
                I.DESCRIPTION DESCRIPTION,
                IL.ITEM_TYPE ITEM_TYPE,
                CHARTOROWID (BD.ROWID) DTL_ROW,
                BH.ORDER_NUM,
                BH.PROD_ORDER_NUM,
                BH.TIE_NUMBER,
                MOD_PART_NUMBER,
                PART_NUMBER,
                DTL_SEQ_NUM
           FROM scdh_REPORTING.OM_BACKLOG_HEADER_STG bh
                JOIN scdh_REPORTING.OM_BACKLOG_DETAIL_STG bd
                   ON     BH.ORDER_NUM = BD.ORDER_NUM
                      AND BH.BU_ID = BD.BU_ID
                      AND BH.PROD_ORDER_NUM = BD.PROD_ORDER_NUM
                      AND BH.PROD_ORDER_LINE = BD.PROD_ORDER_LINE
                      AND BH.TIE_NUMBER = BD.TIE_NUMBER
                      AND BH.REGION_CODE = BD.REGION_CODE
                      AND BH.MOD_NUM = BD.MOD_NUM
                JOIN SCDH_MASTER.MST_ITEM I ON I.ITEM_ID = BD.PART_NUMBER
                JOIN SCDH_MASTER.MST_ITEM_LOCATION IL
                   ON     IL.LOCATION_ID = BH.CCN
                      AND I.ITEM_ID = IL.ITEM_ID
                      AND IL.SYS_ENT_STATE = 'ACTIVE'
          WHERE     BH.region_code = p_region
                AND BH.JOB_TIME = 'HOURLY'
                AND BH.MOD_NUM = p_mod;                                     --

      CURSOR CUR_SYS_MAKE_PICK (
         p_region    scdh_REPORTING.OM_BACKLOG_HEADER_STG.region_code%TYPE,
         p_mod       SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE)
      IS
         SELECT CHARTOROWID (hdr_rowid) H_ROWID,
                CHARTOROWID (dtl_rowid) D_ROWID,
                CASE
                   WHEN SYSTEM_TYPE_CODE = 'SYS' THEN 'OPS-MFG'
                   ELSE 'OPS-NON-MFG'
                END
                   AS BACKLOG_TYPE,
                CASE
                   WHEN MAX (
                           CASE
                              WHEN SYSTEM_TYPE_CODE = 'SYS' THEN 2
                              ELSE 1
                           END)
                        OVER (
                           PARTITION BY REGION_CODE,
                                        ORDER_NUM,
                                        PROD_ORDER_NUM) = 2
                   THEN
                      'POS'
                   ELSE
                      'APOS'
                END
                   AS POS_APOS,
                CASE
                   WHEN SYSTEM_TYPE_CODE = 'SYS' THEN 'TIED'
                   ELSE 'NON-TIED'
                END
                   TIE_GROUP,
                CASE
                   WHEN SYSTEM_TYPE_CODE = 'NON-SYS'
                   THEN
                      'PICK'
                   WHEN MAX (
                           CASE
                              WHEN     SYSTEM_TYPE_CODE = 'SYS'
                                   AND FGA_ID IS NOT NULL
                                   AND NVL (ssc_name, 'XX') IN ('BTS', 'BTP')
                                   AND ff_type_name_BH = 'MFG'
                              THEN
                                 2
                              ELSE
                                 1
                           END)
                        OVER (PARTITION BY region_code,
                                           order_num,
                                           prod_order_num,
                                           tie_number) = 2
                   THEN
                      'SYS-MAKE'
                   WHEN MAX (
                           CASE
                              WHEN     SYSTEM_TYPE_CODE = 'SYS'
                                   AND FGA_ID IS NOT NULL
                                   AND NVL (ssc_name, 'XX') IN ('BTS', 'BTP')
                              THEN
                                 2
                              ELSE
                                 1
                           END)
                        OVER (PARTITION BY region_code,
                                           order_num,
                                           prod_order_num,
                                           tie_number) = 2
                   THEN
                      'PICK'
                   WHEN MAX (
                           CASE
                              WHEN     SYSTEM_TYPE_CODE = 'SYS'
                                   AND ff_type_name_BD = 'MFG'
                              THEN
                                 2
                              ELSE
                                 1
                           END)
                        OVER (PARTITION BY region_code,
                                           order_num,
                                           prod_order_num,
                                           tie_number) = 2
                   THEN
                      'SYS-MAKE'
                   WHEN     SYSTEM_TYPE_CODE = 'SYS'
                        AND COUNT (
                               DISTINCT TIE_NUMBER)
                            OVER (
                               PARTITION BY region_code,
                                            order_num,
                                            prod_order_num) = 1
                        AND ff_type_name_BH = 'MFG'
                   THEN
                      'SYS-MAKE'
                   WHEN SYSTEM_TYPE_CODE = 'SYS' /* AND COUNT(DISTINCT TIE_NUMBER)
                        OVER (PARTITION BY region_code,order_number,prod_order_number) = 1 */
                                                AND ff_type_name_BH <> 'MFG'
                   THEN
                      'PICK'
                   WHEN     SYSTEM_TYPE_CODE = 'SYS'
                        AND COUNT (part_NUMBER)
                               OVER (PARTITION BY region_code,
                                                  order_num,
                                                  prod_order_num,
                                                  tie_number) = 0
                        AND ff_type_name_BH = 'MFG'
                   THEN
                      'SYS-MAKE'
                   ELSE
                      'UNK'
                END
                   AS pick_make,
                CASE
                   WHEN     ff_type_name_BH = 'MFG'
                        AND organization_name_bh <> 'DELL'
                   THEN
                      'ODM'
                   WHEN     ff_type_name_BH = 'MFG'
                        AND organization_name_bh = 'DELL'
                   THEN
                      'DELL-FACTORY'
                   ELSE
                      ff_type_name_BH
                END
                   FACILITY_TYPE,
                SYS_MAKE_PICK.*
           FROM (SELECT ROWIDTOCHAR (bh.ROWID) hdr_rowid,
                        ROWIDTOCHAR (bd.ROWID) dtl_rowid,
                        bh.region_code,
                        bh.bu_id,
                        bh.order_num,
                        MGF.SUB_RGN_CODE SUB_RGN_CDE,
                        bh.prod_order_num,
                        bh.TIE_NUMBER,
                        bh.ssc_name,
                        bd.part_number,
                        bd.mod_part_number,
                        bh.job_time job_time,
                        f_bd.ff_type_name ff_type_name_BD,
                        f_bh.ff_type_name ff_type_name_BH,
                        f_bh.ORGANIZATION_NAME organization_name_bh,
                        CASE
                           WHEN MAX (
                                   CASE
                                      WHEN    BD.IS_FGA_SKU = 'Y'
                                           OR P_bh.SYS_TYPE_CODE IS NOT NULL
                                           OR P_bd.SYS_TYPE_CODE IS NOT NULL
                                           OR (    BD.IL_ITEM_TYPE = '06'
                                               AND BD.I_DESCRIPTION LIKE
                                                      '%BASE%')
                                           OR NVL (CM_FGA.FGA_ID,
                                                   CM_FGA_BD.FGA_ID)
                                                 IS NOT NULL
                                      THEN
                                         2
                                      ELSE
                                         1
                                   END)
                                OVER (PARTITION BY BH.REGION_CODE,
                                                   BH.ORDER_NUM,
                                                   BH.PROD_ORDER_NUM,
                                                   BH.TIE_NUMBER) = 2
                           THEN
                              'SYS'
                           ELSE
                              'NON-SYS'
                        END
                           AS SYSTEM_TYPE_CODE,
                        BD.IL_RETURN_ON_ASN RETURN_ON_ASN,
                        CASE
                           WHEN BH.SSC_NAME IS NOT NULL
                           THEN
                              'SMART SELECTION'
                           WHEN P_BH.BASE_PROD_CODE LIKE '%BTX%'
                           THEN
                              'SMART SELECTION'
                           WHEN MAX (
                                   CASE
                                      WHEN P_BD.BASE_PROD_CODE LIKE '%BTX%'
                                      THEN
                                         2
                                      ELSE
                                         1
                                   END)
                                OVER (PARTITION BY BH.REGION_CODE,
                                                   BH.ORDER_NUM,
                                                   BH.PROD_ORDER_NUM,
                                                   BH.TIE_NUMBER) = 2
                           THEN
                              'SMART SELECTION'
                           ELSE
                              'CONFIGURABLE'
                        END
                           AS SS_TYPE_NAME,
                        CASE
                           WHEN     bh.TP_FACILITY =
                                       NVL (bh.SHIP_TO_FACILITY, 'xx')
                                AND f_bh.FF_TYPE_NAME = 'MFG'
                           THEN
                              'Y'
                           ELSE
                              'N'
                        END
                           AS DIRECT_SHIP_FLAG,
                        CASE
                           WHEN     MAX (
                                       CASE
                                          WHEN bd.part_type_code = 'K' THEN 2
                                          ELSE 1
                                       END)
                                    OVER (PARTITION BY bh.region_code,
                                                       bh.order_num,
                                                       bh.prod_order_num,
                                                       bh.tie_number) = 2
                                AND MAX (
                                       CASE
                                          WHEN f_bd.ff_type_name = 'MFG'
                                          THEN
                                             2
                                          ELSE
                                             1
                                       END)
                                    OVER (
                                       PARTITION BY bh.region_code,
                                                    bh.order_num,
                                                    bh.prod_order_num) = 1
                                AND (    f_bd.site_name IN (SELECT propvalue
                                                              FROM FDL_SNOP_SCDHUB.MST_SYS_PROPS
                                                             WHERE id =
                                                                      'PPB2-SITE_UNDER_FACILITY')
                                     AND f_bd.ff_type_name <> 'MFG')
                                AND bh.base_type IN (SELECT propvalue
                                                       FROM FDL_SNOP_SCDHUB.MST_SYS_PROPS
                                                      WHERE id =
                                                               'PPB2-BASE_TYPE')
                                AND bd.part_type_code IN (SELECT propvalue
                                                            FROM FDL_SNOP_SCDHUB.MST_SYS_PROPS
                                                           WHERE id =
                                                                    'PPB2-PART_TYPE')
                                AND NVL (bh.ssc_name, 'XX') NOT IN ('BTS',
                                                                    'BTP')
                           THEN
                              'Y'
                           ELSE
                              'N'
                        END
                           AS is_supressed,
                        NULL order_source,
                        bh.base_type,
                        NVL (
                           p_bh.BASE_PROD_PROT_ID,
                           FIRST_VALUE (P_BD.BASE_PROD_PROT_ID IGNORE NULLS)
                              OVER (PARTITION BY BH.REGION_CODE,
                                                 BH.ORDER_NUM,
                                                 BH.PROD_ORDER_NUM,
                                                 BH.TIE_NUMBER))
                           BASE_PROD_PROT_ID,
                        NVL (p_bh.BASE_PROD_CODE,
                             FIRST_VALUE (P_BD.BASE_PROD_CODE IGNORE NULLS)
                                OVER (PARTITION BY BH.REGION_CODE,
                                                   BH.ORDER_NUM,
                                                   BH.PROD_ORDER_NUM,
                                                   BH.TIE_NUMBER))
                           BASE_PROD_CODE,
                        NVL (p_bh.FMLY_PARNT_CODE,
                             FIRST_VALUE (P_BD.FMLY_PARNT_CODE IGNORE NULLS)
                                OVER (PARTITION BY BH.REGION_CODE,
                                                   BH.ORDER_NUM,
                                                   BH.PROD_ORDER_NUM,
                                                   BH.TIE_NUMBER))
                           FMLY_PARNT_NAME,
                        NULL STAGE,
                        NULL SUBSTAGE,
                        NULL BACKLOG_FACILITY,
                        NULL CONSUMPTION_STATUS,
                        BD.DTL_SEQ_NUM,
                        CASE
                           WHEN BH.ssc_name IS NULL
                           THEN
                              NULL
                           ELSE
                              NVL (
                                 NVL (CM_FGA.FGA_ID, CM_FGA_BD.FGA_ID),
                                 FIRST_VALUE (
                                    NVL (CM_FGA.FGA_ID, CM_FGA_BD.FGA_ID) IGNORE NULLS)
                                 OVER (PARTITION BY BH.REGION_CODE,
                                                    BH.ORDER_NUM,
                                                    BH.PROD_ORDER_NUM,
                                                    BH.TIE_NUMBER))
                        END
                           AS FGA_ID,
                        NVL (ft.FGA_ID, 'UNK') FASTTRACK_CONFIG_ID
                   FROM scdh_REPORTING.OM_BACKLOG_HEADER_STG bh
                        LEFT JOIN scdh_REPORTING.OM_BACKLOG_DETAIL_STG bd
                           ON     BH.ORDER_NUM = BD.ORDER_NUM
                              AND BH.BU_ID = BD.BU_ID
                              AND BH.PROD_ORDER_NUM = BD.PROD_ORDER_NUM
                              AND BH.PROD_ORDER_LINE = BD.PROD_ORDER_LINE
                              AND BH.TIE_NUMBER = BD.TIE_NUMBER
                              AND BH.REGION_CODE = BD.REGION_CODE
                              AND BH.MOD_NUM = BD.MOD_NUM
                        LEFT JOIN FDL_SNOP_SCDHUB.MST_GEOGRAPHY_FLAT MGF
                           ON     MGF.BU_ID = bh.BU_ID
                              AND MGF.sys_ent_state = 'ACTIVE'
                        LEFT JOIN SCDH_MASTER.SKU_TO_FASTTRACK ft
                           ON     ft.item_num = NVL (bh.base_sku, bd.SKU_NUM)
                              AND ft.SYS_ENT_STATE = 'ACTIVE'
                              AND ft.ITEM_TYPE = 'SKU'
                              AND ft.sub_rgn_code = MGF.SUB_RGN_CODE
                        LEFT JOIN SCDH_MASTER.DELL_CM_FGA_REF CM_FGA
                           ON     (   (CM_FGA.FGA_PROXY_SKU = BH.BASE_SKU)
                                   OR (CM_FGA.FGA_PROXY_MOD = BH.BASE_SKU)
                                   OR (CM_FGA.FGA_ID = BH.BASE_SKU))
                              /* OR (CM_FGA.FGA_PROXY_SKU =BD.SKU_NUM)
                               OR (CM_FGA.FGA_PROXY_MOD IN (BD.SKU_NUM,BD.MOD_PART_NUMBER,BD.PART_NUMBER))
                               OR (CM_FGA.FGA_ID IN (BD.SKU_NUM,BD.MOD_PART_NUMBER,BD.PART_NUMBER)))*/
                              AND CM_FGA.sys_ent_state = 'ACTIVE'
                        LEFT JOIN SCDH_MASTER.DELL_CM_FGA_REF CM_FGA_BD
                           ON     (   (CM_FGA_BD.FGA_PROXY_SKU = BD.SKU_NUM)
                                   OR (CM_FGA_BD.FGA_PROXY_MOD IN (BD.SKU_NUM,
                                                                   BD.MOD_PART_NUMBER,
                                                                   BD.PART_NUMBER))
                                   OR (CM_FGA_BD.FGA_ID IN (BD.SKU_NUM,
                                                            BD.MOD_PART_NUMBER,
                                                            BD.PART_NUMBER)))
                              AND CM_FGA_BD.sys_ent_state = 'ACTIVE'
                        LEFT JOIN FDL_SNOP_SCDHUB.MST_SKU S_bh
                           ON     S_bh.SKU_NUM = BH.BASE_SKU
                              AND NVL (S_bh.SYS_ENT_STATE, 'ACTIVE') =
                                     'ACTIVE'
                        LEFT JOIN FDL_SNOP_SCDHUB.MST_PRODUCT_FLAT P_bh
                           ON P_bh.ITM_CLS_CODE = S_bh.ITEM_CLASS_ID
                        --AND p_bh.sys_ent_state = 'ACTIVE'
                        LEFT JOIN FDL_SNOP_SCDHUB.MST_SKU S_bd
                           ON     S_bd.SKU_NUM = BD.SKU_NUM
                              AND NVL (S_bd.SYS_ENT_STATE, 'ACTIVE') =
                                     'ACTIVE'
                        LEFT JOIN FDL_SNOP_SCDHUB.MST_PRODUCT_FLAT P_bd
                           ON P_bd.ITM_CLS_CODE = S_bd.ITEM_CLASS_ID
                        --AND p_bD.sys_ent_state = 'ACTIVE'
                        LEFT JOIN scdh_master.facility f_bh
                           ON     bh.tp_facility = f_bh.facility_code
                              AND f_bh.sys_ent_state = 'ACTIVE'
                        LEFT JOIN scdh_master.facility f_bd
                           ON     bd.consumption_facility =
                                     f_bd.facility_code
                              AND f_bd.sys_ent_state = 'ACTIVE'
                  WHERE bh.region_code = p_region AND bh.mod_num = p_mod)
                SYS_MAKE_PICK;

      --- ADDED THE BELOW CURSOR FOR OTM_APJ CHANGES -- commented as CUR_AN will take care)
      --OTM_APJ CHANGES STARTS
      --           CURSOR CUR_AN_APJ(
      --       p_region scdh_REPORTING.OM_BACKLOG_HEADER_STG.region_code%TYPE,
      --       p_mod    SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE    )
      --      IS
      --      SELECT BH.ORDER_NUM,
      --CHARTOROWID (BH.ROWID) HDR_ROW,
      --AN.MSG_TYPE MSG_TYPE
      --  FROM SCDH_REPORTING.OM_BACKLOG_HEADER_STG BH
      --       JOIN SCDH_REPORTING.ASN_NOTIFICATION AN
      --          ON AN.ASN_NUMBER = BH.ASN_ID AND BH.REGION_CODE = AN.REGION
      --          AND AN.REGION = 'APJ'
      --       JOIN SCDH_FULFILLMENT.SALES_ORDER SO
      --          ON BH.ORDER_NUM = SO.SALES_ORDER_ID
      --          AND BH.REGION_CODE = SO.REGION_CODE
      --       JOIN SCDH_FULFILLMENT.SO_ATTRIBUTE SA
      --          ON     SA.SALES_ORDER_REF = SO.SALES_ORDER_REF
      --             AND SA.ATTRIBUTE_NAME IN ( SELECT PROPVALUE
      --                                          FROM FDL_SNOP_SCDHUB.MST_SYS_PROPS
      --                                         WHERE ID = 'IS_OTM_ENABLED')
      --             AND SA.ATTRIBUTE_VALUE = 'Y'
      -- WHERE     BH.REGION_CODE = P_REGION
      --       AND BH.JOB_TIME = 'HOURLY'
      --       AND AN.MSG_TYPE IN
      --              (SELECT PROPVALUE
      --                 FROM FDL_SNOP_SCDHUB.MST_SYS_PROPS
      --                WHERE ID = 'MERGE_APJ' AND PROPNAME = 'ASN_STATUS')
      --       AND BH.MOD_NUM = P_MOD;
      --
      --        TYPE coll_AN_APJ IS TABLE OF CUR_AN_APJ%ROWTYPE;
      --           t_AN_APJ             coll_AN_APJ;
      --OTM_APJ CHANGES ENDS

      ------------------------CURSOR FOR DELETING ROWS FROM OM HEADER-------------------------------------------------
      CURSOR cur_delete_sync (
         p_region     IN SCDH_REPORTING.OM_BACKLOG_HEADER.REGION_CODE%TYPE,
         p_mod        IN SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE, --
         p_job_time   IN SCDH_REPORTING.OM_BACKLOG_HEADER.job_time%TYPE)
      IS
         SELECT DELETED_ORD.ORDER_NUMBER ORDER_NUMBER,
                prod_order_num,
                BU_ID,
                TIE_NUMBER
           FROM (SELECT ORDER_NUM AS ORDER_NUMBER,
                        prod_order_num,
                        BU_ID,
                        TIE_NUMBER
                   FROM SCDH_REPORTING.OM_BACKLOG_HEADER
                  WHERE     REGION_CODE = p_region
                        AND job_time = p_job_time
                        AND MOD (ORDER_NUM, 10) = p_mod
                 MINUS
                 SELECT ORDER_NUMBER,
                        prod_order_number,
                        BUID AS BU_ID,
                        TIE_NUMBER
                   FROM SCDH_FULFILLMENT.BACKLOG_HEADER
                  WHERE     region_code = p_region
                        AND MOD (ORDER_NUMBER, 10) = p_mod
                        ) DELETED_ORD;

      CURSOR cur_hdr_stg_to_main (
         p_region    SCDH_FULFILLMENT.BACKLOG_HEADER.REGION_CODE%TYPE,
         p_mod       SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE)
      IS
         SELECT *
           FROM scdh_reporting.OM_BACKLOG_HEADER_STG
          WHERE REGION_CODE = P_REGION AND MOD_NUM = p_mod;

      t_get_order_tab             SCDH_REPORTING.TAB_VARCHAR;


      TYPE coll_hdr_stg_to_main IS TABLE OF cur_hdr_stg_to_main%ROWTYPE;

      t_hdr_stg_to_main           coll_hdr_stg_to_main;

      TYPE rec_manifest IS RECORD
      (
         asn_id                   scdh_reporting.OM_BACKLOG_HEADER_STG.asn_id%TYPE,
         asn_status_code          scdh_reporting.OM_BACKLOG_HEADER_STG.asn_status_code%TYPE,
         wo_status_code           scdh_reporting.OM_BACKLOG_HEADER_STG.wo_status_code%TYPE,
         ship_notification_date   scdh_reporting.OM_BACKLOG_HEADER_STG.ship_notification_date%TYPE,
         channel_status_code      scdh_reporting.OM_BACKLOG_HEADER_STG.channel_status_code%TYPE,
         region_code              scdh_reporting.OM_BACKLOG_HEADER_STG.region_code%TYPE,
         order_num                scdh_reporting.OM_BACKLOG_HEADER_STG.order_num%TYPE,
         bu_id                    scdh_reporting.OM_BACKLOG_HEADER_STG.bu_id%TYPE,
         goods_receipt_date       scdh_reporting.OM_BACKLOG_HEADER_STG.goods_receipt_date%TYPE,
         manifest_ship_date       scdh_reporting.OM_BACKLOG_HEADER_STG.manifest_ship_date%TYPE
      );

      TYPE coll_wo_mf_bcklog IS TABLE OF rec_manifest;

      t_wo_mf_bcklog              coll_wo_mf_bcklog := coll_wo_mf_bcklog ();

      TYPE coll_SYS_MAKE_UNK IS TABLE OF CUR_SYS_MAKE_UNK%ROWTYPE;

      t_sys_make_unk              coll_sys_make_unk;

      -----Date:27-Apr-14 Comment:This type was put due to base_prod_code,BASE_PROD_PROT_ID,FMLY_PARNT_CODE coming as NULL.
      TYPE coll_PROT_ID_NULL IS TABLE OF CUR_PROT_ID_NULL%ROWTYPE;

      t_prot_id_null              coll_PROT_ID_NULL;

      TYPE coll_sys_make_pick IS TABLE OF CUR_SYS_MAKE_PICK%ROWTYPE;

      t_sys_make_pick             coll_sys_make_pick;

      TYPE coll_item_loc IS TABLE OF CUR_ITEM_LOC%ROWTYPE;

      t_item_loc                  coll_item_loc;

      TYPE coll_delete_sync IS TABLE OF cur_delete_sync%ROWTYPE
         INDEX BY PLS_INTEGER;

      t_delete_sync_tab           coll_delete_sync;

      rc_manifest                 CurTyp;

      P_backlog_FCLTY_condition   scdh_reporting.tab_of_condition
                                     := scdh_reporting.tab_of_condition ();
      P_CON_STS_condition         scdh_reporting.tab_of_condition
                                     := scdh_reporting.tab_of_condition ();

      VAR_ERR_CODE                VARCHAR2 (4000);
      VAR_ERR_MSG                 VARCHAR2 (4000);
      -- V_ORDER_SOURCE              scdh_reporting.OM_BACKLOG_HEADER.ORDER_SOURCE%TYPE;
      lv_tot_rec_cnt              NUMBER := 0;
      lv_selcted_rec              NUMBER := 0;
      lv_insert_rec               NUMBER := 0;
      lv_updated_rec              NUMBER := 0;
      lv_deletd_rec               NUMBER := 0;
      lv_indx_1                   NUMBER := 0;
      lv_cnt                      NUMBER := 0;
      lv_result                   BOOLEAN;
      exp_post                    EXCEPTION;
      V_OMS_SOURCE1               VARCHAR2 (50) := NULL;
      V_OMS_SOURCE2               VARCHAR2 (50) := NULL;
      lv_job_audit_log_id_1       fdl_snop_scdhub.process_job_detail.job_audit_log_id%TYPE;
      lv_job_audit_log_id_2       fdl_snop_scdhub.process_job_detail.job_audit_log_id%TYPE;
      rec_cnt_header              NUMBER := 0;
      lv_exception_cnt            NUMBER := 0;
      lv_exEP_REC_counter         NUMBER := 0;
      v_table_name                VARCHAR2 (50);
      v_partition_name            VARCHAR2 (50);
      lv_manifest_qry             VARCHAR2 (4000);
      p_mod_trunc                 NUMBER := 0;

      /* Added the below cursor as part of OM Merge Backlog program. Below cursor will update SHIP_STATUS_CODe in the STG table*/
      CURSOR CUR_OSS (
         p_region    scdh_REPORTING.OM_BACKLOG_HEADER_STG.region_code%TYPE,
         p_mod       SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE)
      IS
         SELECT BH.ORDER_NUM,
                CHARTOROWID (BH.ROWID) HDR_ROW,
                OSS.SHIP_STATUS_CODE SHIP_STATUS_CODE
           FROM scdh_REPORTING.OM_BACKLOG_HEADER_STG bh
                JOIN SCDH_REPORTING.ORDER_SHIPMENT_STATUS OSS
                   ON     OSS.ORDER_NUM = bh.ORDER_NUM
                      AND BH.region_code = OSS.REGION
          WHERE     BH.region_code = p_region
                AND BH.JOB_TIME = 'HOURLY'
                AND OSS.SHIP_STATUS_CODE IN (SELECT propvalue
                                               FROM fdl_snop_scdhub.mst_sys_props
                                              WHERE id = 'MERGE_EMEA')
                AND BH.MOD_NUM = p_mod;

      TYPE coll_OSS IS TABLE OF CUR_OSS%ROWTYPE;

      t_OSS                       coll_OSS;

      --------------------DP ENABLED FLAG CHANGES BEGIN

      CURSOR cur_get_so_lead (
         p_region    scdh_REPORTING.OM_BACKLOG_HEADER_STG.region_code%TYPE,
         p_mod       SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE)
      IS
         SELECT DISTINCT OBST.ORDER_NUM,OBST.BU_ID, CHARTOROWID (OBST.ROWID) HDR_ROW
            FROM scdh_REPORTING.OM_BACKLOG_HEADER_STG OBST
             WHERE   EXISTS (SELECT 1 FROM SCDH_FULFILLMENT.SO_LEAD_TIME SOL
                        WHERE  SOL.DELL_RGN_CD = OBST.region_code
                            AND OBST.JOB_TIME = 'HOURLY'
                            AND OBST.region_code = p_region
                            AND OBST.ORDER_NUM = SOL.SO_NBR
                            AND OBST.BU_ID = SOL.BU_ID
                            AND SOL.VER_NUM=1
                            AND OBST.MOD_NUM = p_mod );

      TYPE coll_so_lead IS TABLE OF cur_get_so_lead%ROWTYPE;

      t_so_lead                   coll_so_lead;


      --------------------DP ENABLED FLAG CHANGES END
      --Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3) begin
      CURSOR Cur_get_FDD_APJ (
         p_region    scdh_REPORTING.OM_BACKLOG_HEADER_STG.region_code%TYPE,
         p_mod       SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE)
      IS
         /*SELECT bh.ORDER_NUM,
                CHARTOROWID (BH.ROWID) HDR_ROW,
                fdd.FDD_FLAG,
                fdd.FDD_DATE,
                BH.MUST_ARRIVE_BY_DATE
           FROM scdh_REPORTING.OM_BACKLOG_HEADER_STG bh
                JOIN scdh_fulfillment.STG_DRGN_ORD_HEADER fdd
                   ON fdd.ORDER_NUM = bh.ORDER_NUM
          WHERE     BH.region_code = p_region
                AND BH.JOB_TIME = 'HOURLY'
                AND BH.MOD_NUM = p_mod;*/
-- commented  above and add below code for Story#6741082 - Change in logic for setting FDD Flag and FDD Date based upon So_attribute table 
SELECT ORDER_NUM,
       HDR_ROW,
       FDD_FLAG,
       TO_DATE (FDD_DATE, 'MM/DD/YYYY HH12:MI:SS AM') AS FDD_DATE
  FROM (SELECT OM.ORDER_NUM AS ORDER_NUM,
               CHARTOROWID (OM.ROWID) HDR_ROW,
               SA.ATTRIBUTE_NAME AS SNAME,
               SA.ATTRIBUTE_VALUE AS SVALUE
          FROM SCDH_FULFILLMENT.SO_ATTRIBUTE SA
               INNER JOIN SCDH_FULFILLMENT.SALES_ORDER SO
                  ON SO.SALES_ORDER_REF = SA.SALES_ORDER_REF
               INNER JOIN
               SCDH_REPORTING.OM_BACKLOG_HEADER_STG OM
                  ON     OM.REGION_CODE = SO.REGION_CODE
                     AND SO.SALES_ORDER_ID = OM.ORDER_NUM
         WHERE     OM.region_code = p_region
               AND OM.JOB_TIME = 'HOURLY'
               AND OM.MOD_NUM = p_mod
               AND UPPER (ATTRIBUTE_NAME) IN
                      ('FIXED_DELIVERY_FLAG', 'FIX_DELIVERY_DATE')) PIVOT (MAX (
                                                                              SVALUE)
                                                                    FOR SNAME
                                                                    IN  ('FIXED_DELIVERY_FLAG' AS FDD_FLAG,
                                                                        'FIX_DELIVERY_DATE' AS FDD_DATE));
 /*WHERE     FDD_FLAG = 'Y'
       AND FDD_DATE IS NOT NULL
       AND FDD_DATE <> '1/1/0001 12:00:00 AM'; */                                                                                                                                
      TYPE coll_FDD_APJ IS TABLE OF Cur_get_FDD_APJ%ROWTYPE;

      t_FDD_APJ                   coll_FDD_APJ;



      CURSOR Cur_get_FDD (
         p_region    scdh_REPORTING.OM_BACKLOG_HEADER_STG.region_code%TYPE,
         p_mod       SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE)
      IS
         SELECT
                hd.order_num order_num
--                ,CHARTOROWID (hd.ROWID) HDR_ROW
           FROM scdh_reporting.om_backlog_header_stg hd
                INNER JOIN scdh_outbound.sales_order_vw so
                   ON     hd.order_num = so.sales_order_id
                      AND hd.bu_id = SO.buid
                      AND hd.region_code = so.region_code
                      AND hd.job_time = 'HOURLY'
                INNER JOIN scdh_outbound.PROD_ORDER_VW PO
                   ON     SO.SALES_ORDER_ID = PO.SALES_ORDER_ID
                      AND SO.BUID = PO.BUID
                      AND SO.REGION_CODE = PO.REGION_CODE
                INNER JOIN scdh_outbound.PROD_ORDER_LINE_VW POL
                   ON     PO.PROD_ORDER_NUM = POL.PROD_ORDER_NUM
                      AND PO.REGION_CODE = POL.REGION_CODE
                INNER JOIN scdh_outbound.LINE_SKU_VW LS
                   ON     POL.PROD_ORDER_NUM = LS.PROD_ORDER_NUM
                      AND POL.LINE_NUM = LS.LINE_NUM
                      AND LS.REGION_CODE =  hd.REGION_CODE
                      AND EXISTS (select 'x' from SCDH_MASTER.FDD_SKU_MOD fsm
                                    where sys_ent_state = 'ACTIVE'
                                    AND (fsm.SKU_MOD_NBR = LS.mfg_part_num
                                         OR fsm.SKU_MOD_NBR = LS.SKU_NUM
                                         ) )
          WHERE hd.MOD_NUM = p_mod AND hd.region_code = p_region
    UNION
        SELECT
            hd.order_num
--            ,CHARTOROWID (hd.ROWID) HDR_ROW
        FROM scdh_reporting.om_backlog_header_stg hd
                INNER JOIN scdh_outbound.sales_order_vw so
                   ON     hd.order_num = so.sales_order_id
                      AND hd.bu_id = SO.buid
                      AND hd.region_code = so.region_code
                      AND hd.job_time = 'HOURLY'
                INNER JOIN scdh_outbound.PROD_ORDER_VW PO
                   ON     SO.SALES_ORDER_ID = PO.SALES_ORDER_ID
                      AND SO.BUID = PO.BUID
                      AND SO.REGION_CODE = PO.REGION_CODE
                INNER JOIN scdh_outbound.PROD_ORDER_LINE_VW POL
                   ON     PO.PROD_ORDER_NUM = POL.PROD_ORDER_NUM
                      AND PO.REGION_CODE = POL.REGION_CODE
                INNER JOIN  scdh_fulfillment.SALES_ORDER_SERVICE_SKU SOSS
                      ON SOSS.SYS_SOURCE = 'FDL_'||HD.REGION_CODE
                      AND so.SALES_ORDER_REF = SOSS.SALES_ORDER_REF
                      AND EXISTS (select 'x' from SCDH_MASTER.FDD_SKU_MOD fsm
                                    where sys_ent_state = 'ACTIVE'
                                    AND (fsm.SKU_MOD_NBR = soss.mfg_part_num
                                         OR fsm.SKU_MOD_NBR = soss.SKU_NUM) )
          WHERE hd.MOD_NUM = p_mod AND hd.region_code = p_region;

      TYPE coll_FDD IS TABLE OF Cur_get_FDD%ROWTYPE;

      t_FDD                       coll_FDD;

           CURSOR Cur_get_FDD_OPR (
         p_region    scdh_REPORTING.OM_BACKLOG_HEADER_STG.region_code%TYPE,
         p_mod       SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE)
      IS
        SELECT
                hd.order_num order_num
--                ,CHARTOROWID (hd.ROWID) HDR_ROW
           FROM scdh_reporting.om_backlog_header_stg hd
                INNER JOIN scdh_outbound.sales_order_vw so
                   ON     hd.order_num = so.sales_order_id
                      AND hd.bu_id = SO.buid
                      AND hd.region_code = so.region_code
                      AND hd.job_time = 'HOURLY'
                INNER JOIN scdh_fulfillment.prod_order_opr PO
                   ON     SO.SALES_ORDER_ID = PO.SALES_ORDER_ID
                      AND SO.BUID = PO.BUID
                      AND SO.REGION_CODE = PO.REGION_CODE
                INNER JOIN scdh_fulfillment.prod_order_line_opr POL
                   ON     PO.PROD_ORDER_NUM = POL.PROD_ORDER_NUM
                      AND PO.REGION_CODE = POL.REGION_CODE
                INNER JOIN scdh_fulfillment.line_sku_opr LS
                   ON     POL.PROD_ORDER_NUM = LS.PROD_ORDER_NUM
                      AND POL.LINE_NUM = LS.LINE_NUM
                      AND LS.REGION_CODE =  hd.REGION_CODE
                      AND EXISTS (select 'x' from SCDH_MASTER.FDD_SKU_MOD fsm
                                    where sys_ent_state = 'ACTIVE'
                                    AND (fsm.SKU_MOD_NBR = LS.mfg_part_num
                                         OR fsm.SKU_MOD_NBR = LS.SKU_NUM
                                         ) )
          WHERE hd.MOD_NUM = p_mod AND hd.region_code = p_region
    UNION
        SELECT
            hd.order_num
--            ,CHARTOROWID (hd.ROWID) HDR_ROW
        FROM scdh_reporting.om_backlog_header_stg hd
                INNER JOIN scdh_outbound.sales_order_vw so
                   ON     hd.order_num = so.sales_order_id
                      AND hd.bu_id = SO.buid
                      AND hd.region_code = so.region_code
                      AND hd.job_time = 'HOURLY'
                INNER JOIN scdh_fulfillment.prod_order_opr PO
                   ON     SO.SALES_ORDER_ID = PO.SALES_ORDER_ID
                      AND SO.BUID = PO.BUID
                      AND SO.REGION_CODE = PO.REGION_CODE
                INNER JOIN scdh_fulfillment.prod_order_line_opr POL
                   ON     PO.PROD_ORDER_NUM = POL.PROD_ORDER_NUM
                      AND PO.REGION_CODE = POL.REGION_CODE
                INNER JOIN  scdh_fulfillment.SALES_ORDER_SERVICE_SKU_OPR SOSS
                      ON SOSS.SYS_SOURCE = 'FDL_'||HD.REGION_CODE
                      AND so.SALES_ORDER_REF = SOSS.SALES_ORDER_REF
                      AND EXISTS (select 'x' from SCDH_MASTER.FDD_SKU_MOD fsm
                                    where sys_ent_state = 'ACTIVE'
                                    AND (fsm.SKU_MOD_NBR = soss.mfg_part_num
                                         OR fsm.SKU_MOD_NBR = soss.SKU_NUM) )
          WHERE hd.MOD_NUM = p_mod AND hd.region_code = p_region;

      TYPE coll_FDD_OPR IS TABLE OF Cur_get_FDD_OPR%ROWTYPE;

      t_FDD_OPR                 coll_FDD_OPR;




      --Cursor to get OPR orders for FDD_FLAG update end <KR002>



      --Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3) changes end
      /* Added the below cursor as part of OM Merge Backlog program. Below cursor will update STATUS_CODe in the STG table*/
      CURSOR CUR_SDT (
         p_region    scdh_REPORTING.OM_BACKLOG_HEADER_STG.region_code%TYPE,
         p_mod       SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE)
      IS
         SELECT BH.ORDER_NUM,
                CHARTOROWID (BH.ROWID) HDR_ROW,
                SDT.STATUS_CODE STATUS_CODE
           FROM scdh_REPORTING.OM_BACKLOG_HEADER_STG bh
                JOIN SCDH_REPORTING.STG_DRGN_TNT SDT
                   ON     SDT.ORDER_NUM = bh.ORDER_NUM
                      AND BH.region_code = SDT.REGION
          WHERE     BH.region_code = p_region
                AND BH.JOB_TIME = 'HOURLY'
                AND SDT.STATUS_CODE IN (SELECT propvalue
                                          FROM fdl_snop_scdhub.mst_sys_props
                                         WHERE id = 'MERGE_APJ')
                AND BH.MOD_NUM = p_mod;

      TYPE coll_SDT IS TABLE OF CUR_SDT%ROWTYPE;

      t_SDT                       coll_SDT;

      /* Added the below cursor as part of OM Merge Backlog program. Below cursor will update MSG_TYPE in the STG table*/
      CURSOR CUR_AN (
         p_region    scdh_REPORTING.OM_BACKLOG_HEADER_STG.region_code%TYPE,
         p_mod       SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE)
      IS
         SELECT BH.ORDER_NUM,
                CHARTOROWID (BH.ROWID) HDR_ROW,
                AN.MSG_TYPE MSG_TYPE
           FROM scdh_REPORTING.OM_BACKLOG_HEADER_STG bh
                JOIN SCDH_REPORTING.asn_notification AN
                   ON     AN.ASN_NUMBER = bh.ASN_ID
                      AND BH.region_code = AN.REGION
          WHERE     BH.region_code = p_region
                AND BH.JOB_TIME = 'HOURLY'
                AND AN.MSG_TYPE IN (SELECT propvalue
                                      FROM fdl_snop_scdhub.mst_sys_props
                                     WHERE id = 'MERGE_AMER' --AND PROPNAME = 'ASN_STATUS' -- OTM_APJ_CHANGES (commented as cursor is Global)
                                                            )
                AND BH.MOD_NUM = p_mod;

      /*  UNION ALL   --Added as part of GL BTP Program
       SELECT  BH.ORDER_NUM,  chartorowid(BH.ROWID) HDR_ROW,ANAE.MSG_TYPE MSG_TYPE
       FROM scdh_REPORTING.OM_BACKLOG_HEADER_STG bh
       JOIN SCDH_FULFILLMENT.asn_notification_AE ANAE
       ON ANAE.ASN_NUMBER=bh.ASN_ID
        AND BH.region_code=ANAE.REGION
        WHERE BH.region_code = p_region
             AND BH.JOB_TIME='HOURLY'
           AND ANAE.MSG_TYPE IN ( SELECT propvalue
                                   FROM fdl_snop_scdhub.mst_sys_props
                                   WHERE id = 'APJ_EMEA')
          AND BH.MOD_NUM = p_mod; */

      TYPE coll_AN IS TABLE OF CUR_AN%ROWTYPE;

      t_AN                        coll_AN;

      /* Added the below cursor as part of GL BTP program. Below cursor will update SO_ATTRIBUTE in the STG table*/
      CURSOR CUR_SOATB (
         P_REGION    SCDH_fulfillment.sales_order.REGION_CODE%TYPE,
         p_mod       SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE)
      IS
         SELECT BH.ORDER_NUM,
                CHARTOROWID (BH.ROWID) HDR_ROW,
                SO.SALES_ORDER_REF,
                SA.ATTRIBUTE_NAME
           FROM SCDH_REPORTING.OM_BACKLOG_HEADER_STG BH,
                SCDH_FULFILLMENT.SALES_ORDER SO,
                SCDH_FULFILLMENT.SO_ATTRIBUTE SA
          WHERE     BH.ORDER_NUM = SO.SALES_ORDER_ID
                AND BH.REGION_CODE = SO.REGION_CODE
                AND SO.SALES_ORDER_REF = SA.SALES_ORDER_REF
                AND BH.REGION_CODE = P_REGION
                AND BH.JOB_TIME = 'HOURLY'
                AND SA.ATTRIBUTE_NAME IN (SELECT propvalue
                                            FROM fdl_snop_scdhub.mst_sys_props
                                           WHERE id IN ('SO_ATTRIBUTE',
                                                        'IS_OTM_ENABLED')) -- OTM_APJ Added IS_OTM_ENABLED
                AND SA.ATTRIBUTE_VALUE = 'Y'
                AND BH.MOD_NUM = p_mod;

      TYPE coll_SOATB IS TABLE OF CUR_SOATB%ROWTYPE;

      t_SOATB                     coll_SOATB;

      CURSOR CUR_MABD_Y (
         P_REGION    SCDH_fulfillment.sales_order.REGION_CODE%TYPE,
         p_mod       SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE)
      IS
         SELECT BH.ORDER_NUM, CHARTOROWID (BH.ROWID) HDR_ROW,MABD_FLAG
           FROM SCDH_REPORTING.OM_BACKLOG_HEADER_STG BH
          WHERE     REGION_CODE = P_REGION
                AND BH.MOD_NUM = p_mod
                --AND MABD_FLAG = 'Y' --commented above code as part of Story#6741082
                AND FDD_FLAG = 'N' 
                AND NVL (TRUNC (MUST_ARRIVE_BY_DATE), TO_DATE ('1/1/0001', 'DD/MM/YYYY')) > TO_DATE ('1/1/1901', 'DD/MM/YYYY');

      TYPE coll_FDD_MABD_Y IS TABLE OF CUR_MABD_Y%ROWTYPE;

      t_FDD_MABD_Y                coll_FDD_MABD_Y;

      CURSOR CUR_FDD_MABD_AMER_Y2 (
         P_REGION    SCDH_fulfillment.sales_order.REGION_CODE%TYPE,
         p_mod       SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE)
      IS
         SELECT BH.ORDER_NUM, CHARTOROWID (BH.ROWID) HDR_ROW
           FROM SCDH_REPORTING.OM_BACKLOG_HEADER_STG BH
          WHERE     REGION_CODE = P_REGION
                AND BH.MOD_NUM = p_mod
                AND MABD_FLAG = 'Y'
                AND FDD_FLAG = 'Y'
                AND BH.MUST_ARRIVE_BY_DATE > BH.ESD;         -- set FDD_FLAG N

      TYPE coll_FDD_MABD_AMER_Y2 IS TABLE OF CUR_FDD_MABD_AMER_Y2%ROWTYPE;

      t_FDD_MABD_AMER_Y2          coll_FDD_MABD_AMER_Y2;


      CURSOR CUR_FDD_MABD_AMER_Y1 (
         P_REGION    SCDH_fulfillment.sales_order.REGION_CODE%TYPE,
         p_mod       SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE)
      IS
         SELECT BH.ORDER_NUM, CHARTOROWID (BH.ROWID) HDR_ROW
           FROM SCDH_REPORTING.OM_BACKLOG_HEADER_STG BH
          WHERE     REGION_CODE = P_REGION
                AND BH.MOD_NUM = p_mod
                AND MABD_FLAG = 'Y'
                AND FDD_FLAG = 'Y'
                AND BH.MUST_ARRIVE_BY_DATE < BH.ESD;       -- set MABD_FLAG  N

      TYPE coll_FDD_MABD_AMER_Y1 IS TABLE OF CUR_FDD_MABD_AMER_Y1%ROWTYPE;

      t_FDD_MABD_AMER_Y1          coll_FDD_MABD_AMER_Y1;

  --Begin--story#5107581--<RK001>   Mark as Consumed for Non-Tied Orders
     cursor cur_hdr_cons_status_nontied( P_REGION    SCDH_fulfillment.sales_order.REGION_CODE%TYPE,
         p_mod       SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE)
      is
       SELECT  BH.ORDER_NUM ,
          BH.REGION_CODE,
          BH.JOB_TIME,
          BH.MOD_NUM,
          BH.ASN_3PL,
          BH.ASN_3PL_STATUS,
          MF.SOURCE_MANIFEST_ID SOURCE_MANIFEST_ID,
          MF.MANIFEST_STATUS_CODE,
          CHARTOROWID (BH.ROWID) HDR_ROW
         FROM  SCDH_REPORTING.OM_BACKLOG_HEADER_STG BH
       JOIN
       SCDH_FULFILLMENT.WORK_ORDER WO
          ON     WO.REGION_CODE = BH.REGION_CODE
             AND WO.ORDER_NUM = BH.ORDER_NUM
             AND WO.BU_ID = BH.BU_ID
             AND WO.WO_TYPE = 'PICK'
       JOIN
       SCDH_FULFILLMENT.WO_MANIFEST_XREF WMF
          ON     WO.REGION_CODE = WMF.REGION_CODE
             AND WO.WO_ID = WMF.WO_ID
             AND WMF.REGION_CODE = BH.REGION_CODE
       JOIN
       SCDH_FULFILLMENT.MANIFEST MF
          ON     WMF.REGION_CODE = MF.REGION_CODE
             AND WMF.MANIFEST_REF = MF.MANIFEST_REF
             AND WMF.SOURCE_MANIFEST_ID = MF.SOURCE_MANIFEST_ID
           --  AND MF.MANIFEST_STATUS_CODE = 40
       JOIN
       SCDH_MASTER.FACILITY FAC
         ON MF.SHIP_FROM_VENDOR_SITE_ID = FAC.FACILITY_CODE
             AND FAC.FF_TYPE_NAME = 'MERGE'
              AND FAC.SYS_ENT_STATE = 'ACTIVE'
 WHERE     BH.REGION_CODE = P_REGION
          AND BH.JOB_TIME = 'HOURLY'
        AND BH.BACKLOG_ORDER_TIE_TYPE IN('CUST-PICK-OPTIONS', 'CTO-CUST-PICK-FINISHEDGOODS')
         AND BH.MOD_NUM = P_MOD;

          TYPE hdr_upd_cons_nontied IS TABLE OF cur_hdr_cons_status_nontied%ROWTYPE;

      t_hdr_upd_cons_nontied       hdr_upd_cons_nontied;

      cursor cur_cons_status_Nontied( P_REGION    SCDH_fulfillment.sales_order.REGION_CODE%TYPE,
         p_mod       SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE)
      is
          SELECT CASE
          WHEN BD.CONSUMPTION_STATUS = 'UNCONSUMED'
          THEN
             CASE
                WHEN     MF.MANIFEST_STATUS_CODE = 40
                     AND FAC.FF_TYPE_NAME = 'MERGE'
                     AND BH.MERGE_TYPE = 'ACCESSORY'
                     AND BD.PART_TYPE_CODE IN ('B', 'K', 'S', 'O')
                  THEN
                     'CONSUMED'
               ELSE BD.CONSUMPTION_STATUS
               END
          ELSE
             BD.CONSUMPTION_STATUS
          END STATUS,
           MF.SOURCE_MANIFEST_ID SOURCE_MANIFEST_ID,
           MF.MANIFEST_STATUS_CODE,
       BD.REGION_CODE,
       BD.JOB_TIME,
       BD.ORDER_NUM,
       BD.PROD_ORDER_NUM,
       BD.TIE_NUMBER,
       BD.BU_ID,
       BD.MOD_PART_NUMBER,
       BD.PART_NUMBER,
       BD.DTL_SEQ_NUM,
       BD.CONSUMPTION_STATUS,
       BD.CONSUMPTION_FACILITY,
       BD.PART_TYPE_CODE,
       BD.BACKLOG_FACILITY,
       CHARTOROWID (BD.ROWID) HDR_ROW
         FROM SCDH_REPORTING.OM_BACKLOG_DETAIL_STG BD
       JOIN SCDH_REPORTING.OM_BACKLOG_HEADER_STG BH
       ON  BH.REGION_CODE = BD.REGION_CODE
             AND BH.JOB_TIME = BD.JOB_TIME
             AND BH.ORDER_NUM = BD.ORDER_NUM
             AND BH.PROD_ORDER_NUM = BD.PROD_ORDER_NUM
             AND BH.TIE_NUMBER = BD.TIE_NUMBER
             AND BH.BU_ID = BD.BU_ID
             AND BH.REGION_CODE = P_REGION
             AND BD.mod_num = p_mod
       JOIN
       SCDH_FULFILLMENT.WORK_ORDER WO
          ON     WO.REGION_CODE = BD.REGION_CODE
             AND WO.ORDER_NUM = BD.ORDER_NUM
             AND WO.BU_ID = BD.BU_ID
             AND WO.WO_TYPE = 'PICK'
       JOIN
       SCDH_FULFILLMENT.WO_MANIFEST_XREF WMF
          ON     WO.REGION_CODE = WMF.REGION_CODE
             AND WO.WO_ID = WMF.WO_ID
             AND WMF.REGION_CODE = BH.REGION_CODE
       JOIN
       SCDH_FULFILLMENT.MANIFEST MF
          ON     WMF.REGION_CODE = MF.REGION_CODE
             AND WMF.MANIFEST_REF = MF.MANIFEST_REF
             AND WMF.SOURCE_MANIFEST_ID = MF.SOURCE_MANIFEST_ID
           --  AND MF.MANIFEST_STATUS_CODE = 40
       JOIN
       SCDH_MASTER.FACILITY FAC
         ON MF.SHIP_FROM_VENDOR_SITE_ID = FAC.FACILITY_CODE
             AND FAC.FF_TYPE_NAME = 'MERGE'
              AND FAC.SYS_ENT_STATE = 'ACTIVE'
 WHERE     BD.REGION_CODE = P_REGION
          AND BD.JOB_TIME = 'HOURLY'
        AND BH.BACKLOG_ORDER_TIE_TYPE IN('CUST-PICK-OPTIONS', 'CTO-CUST-PICK-FINISHEDGOODS');
     -- AND BH.MERGE_TYPE = 'ACCESSORY'
      -- AND BD.PART_TYPE_CODE IN ('B', 'K', 'S', 'O')
      -- AND BD.CONSUMPTION_STATUS = 'UNCONSUMED' ;

       TYPE col_upd_cons_non IS TABLE OF cur_cons_status_Nontied%ROWTYPE;

      t_col_upd_cons_non        col_upd_cons_non;
     --End--story#5107581--<RK001>

 --Begin--story#5107575-<RK002> Mark as Consumed for System Tied Orders

    CURSOR cur_hdr_cons_status_tied( P_REGION    SCDH_fulfillment.sales_order.REGION_CODE%TYPE,
         p_mod       SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE)
    IS
     SELECT   BH.ORDER_NUM ,
          BH.REGION_CODE,
          BH.JOB_TIME,
          BH.MOD_NUM,
          BH.ASN_3PL,
          BH.ASN_3PL_STATUS,
          MF.SOURCE_MANIFEST_ID SOURCE_MANIFEST_ID,
          MF.MANIFEST_STATUS_CODE,
          CHARTOROWID (BH.ROWID) HDR_ROW
  FROM  SCDH_REPORTING.OM_BACKLOG_HEADER_STG BH
           JOIN
       SCDH_FULFILLMENT.WORK_ORDER WO
          ON     WO.REGION_CODE = BH.REGION_CODE
             AND WO.ORDER_NUM = BH.ORDER_NUM
             AND WO.BU_ID = BH.BU_ID
             AND WO.WO_TYPE = 'PICK'
       JOIN
       SCDH_FULFILLMENT.WO_MANIFEST_XREF WMF
          ON     WO.REGION_CODE = WMF.REGION_CODE
             AND WO.WO_ID = WMF.WO_ID
             AND WMF.REGION_CODE = BH.REGION_CODE
       JOIN
       SCDH_FULFILLMENT.MANIFEST MF
          ON     WMF.REGION_CODE = MF.REGION_CODE
             AND WMF.MANIFEST_REF = MF.MANIFEST_REF
             AND WMF.SOURCE_MANIFEST_ID = MF.SOURCE_MANIFEST_ID
            -- AND MF.MANIFEST_STATUS_CODE = 40
       JOIN
       SCDH_MASTER.FACILITY FAC
          ON MF.SHIP_FROM_VENDOR_SITE_ID = FAC.FACILITY_CODE
             AND FAC.FF_TYPE_NAME = 'MERGE'
             AND FAC.SYS_ENT_STATE = 'ACTIVE'
 WHERE     BH.REGION_CODE = P_REGION
          AND BH.JOB_TIME = 'HOURLY'
       AND BH.BACKLOG_ORDER_TIE_TYPE IN ('CTO-CUST-MAKE', 'BTO-CUST-MAKE')
       AND BH.MOD_NUM = P_MOD;
    TYPE hdr_upd_cons_tied IS TABLE OF cur_hdr_cons_status_tied%ROWTYPE;

      t_hdr_upd_cons_tied        hdr_upd_cons_tied;

      CURSOR cur_cons_status_tied( P_REGION    SCDH_fulfillment.sales_order.REGION_CODE%TYPE,
         p_mod       SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE)
IS

         SELECT CASE
           WHEN BD.CONSUMPTION_STATUS = 'UNCONSUMED'
          THEN
             CASE
                WHEN     MF.MANIFEST_STATUS_CODE = 40
                     AND FAC.FF_TYPE_NAME = 'MERGE'
                     AND BD.CONSUMPTION_FACILITY = BH.SHIP_TO_FACILITY
                     AND BD.PART_TYPE_CODE IN ('B', 'K', 'S', 'O')
                THEN
                   'CONSUMED'
                ELSE BD.CONSUMPTION_STATUS
               END
          ELSE
             BD.CONSUMPTION_STATUS
       END STATUS,
       MF.SOURCE_MANIFEST_ID SOURCE_MANIFEST_ID,
           MF.MANIFEST_STATUS_CODE,
       BD.REGION_CODE,
       BD.JOB_TIME,
       BD.ORDER_NUM,
       BD.PROD_ORDER_NUM,
       BD.TIE_NUMBER,
       BD.BU_ID,
       BD.MOD_PART_NUMBER,
       BD.PART_NUMBER,
       BD.DTL_SEQ_NUM,
       BD.CONSUMPTION_STATUS,
       BD.CONSUMPTION_FACILITY,
       BD.PART_TYPE_CODE,
       BD.BACKLOG_FACILITY,
       CHARTOROWID (BD.ROWID) HDR_ROW
  FROM SCDH_REPORTING.OM_BACKLOG_DETAIL_STG BD
       JOIN
       SCDH_REPORTING.OM_BACKLOG_HEADER_STG BH
          ON     BH.REGION_CODE = BD.REGION_CODE
             AND BH.JOB_TIME = BD.JOB_TIME
             AND BH.ORDER_NUM = BD.ORDER_NUM
             AND BH.PROD_ORDER_NUM = BD.PROD_ORDER_NUM
             AND BH.TIE_NUMBER = BD.TIE_NUMBER
             AND BH.BU_ID = BD.BU_ID
             AND BH.REGION_CODE = P_REGION
             AND BD.mod_num = p_mod
       JOIN
       SCDH_FULFILLMENT.WORK_ORDER WO
          ON     WO.REGION_CODE = BH.REGION_CODE
             AND WO.ORDER_NUM = BH.ORDER_NUM
             AND WO.BU_ID = BH.BU_ID
             AND WO.WO_TYPE = 'PICK'
       JOIN
       SCDH_FULFILLMENT.WO_MANIFEST_XREF WMF
          ON     WO.REGION_CODE = WMF.REGION_CODE
             AND WO.WO_ID = WMF.WO_ID
             AND WMF.REGION_CODE = BH.REGION_CODE
       JOIN
       SCDH_FULFILLMENT.MANIFEST MF
          ON     WMF.REGION_CODE = MF.REGION_CODE
             AND WMF.MANIFEST_REF = MF.MANIFEST_REF
             AND WMF.SOURCE_MANIFEST_ID = MF.SOURCE_MANIFEST_ID
            -- AND MF.MANIFEST_STATUS_CODE = 40
       JOIN
       SCDH_MASTER.FACILITY FAC
          ON MF.SHIP_FROM_VENDOR_SITE_ID = FAC.FACILITY_CODE
             AND FAC.FF_TYPE_NAME = 'MERGE'
             AND FAC.SYS_ENT_STATE = 'ACTIVE'
 WHERE     BH.REGION_CODE = P_REGION
          AND BH.JOB_TIME = 'HOURLY'
       AND BH.BACKLOG_ORDER_TIE_TYPE IN ('CTO-CUST-MAKE', 'BTO-CUST-MAKE');
     --  AND BD.PART_TYPE_CODE IN ('B', 'K', 'S', 'O')
    --   AND BD.CONSUMPTION_FACILITY = BH.SHIP_TO_FACILITY
    --   AND BD.CONSUMPTION_STATUS = 'UNCONSUMED';
       TYPE col_upd_cons_tied IS TABLE OF cur_cons_status_tied%ROWTYPE;

      t_col_upd_cons_tied        col_upd_cons_tied;
      --End--story#5107575---<RK002>
  --Begin --Story#5500245--<RK006>
  CURSOR Cur_ReadyStock_Pick( P_REGION    SCDH_fulfillment.sales_order.REGION_CODE%TYPE,
                              p_mod       SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE)
    IS
     SELECT  ORDER_NUM,PROD_ORDER_NUM,TIE_NUMBER,BU_ID,JOB_TIME,BACKLOG_ORDER_TIE_TYPE,SYSTEM_TYPE_CODE,RSID,OM_ORDER_TYPE,MOD_NUM
    FROM SCDH_REPORTING.OM_BACKLOG_HEADER_STG BH
    WHERE BH.REGION_CODE = P_REGION AND BH.JOB_TIME = 'HOURLY'
    AND BH.MOD_NUM = P_MOD
    AND BH.OM_ORDER_TYPE = 'ReadyStock Pick';

    TYPE Col_ReadyStock_Pick IS TABLE OF Cur_ReadyStock_Pick%ROWTYPE;

    t_Stock_Pick      Col_ReadyStock_Pick;
     --End-Story#5500245--<RK006>

   BEGIN
      -------------------------------------------------------get the job_name and sync_up id-------------------------------------------------------------
      --DBMS_OUTPUT.PUT_LINE('IN PKG');
      lv_error_location := 2.4;
      lv_job_name := 'OM_BACKLOG_HD_DETAIL_' || p_region;
      lv_sync_up_id := 'OM_BACKLOG_HD_DETAIL_' || p_region;
      lv_job_instance_id := p_job_instance_id;
      lv_audit_log_yn := p_audit_log_yn;
      --------------------------------------------------------------------------------------------------------------------------------------------------



      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
            P_STEP_NAME                 => 'START SET UP',
            P_JOB_AUDIT_MESSAGE         =>    'Truncating Staging table partition for mod'
                                           || p_mod
                                           || ' and calcualting stage and sub stage conditions ',
            P_NUMBER_OF_ROWS_SELECTED   => NULL,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_IS_BASE_TABLE             => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'Truncation started..',
            P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
            P_JOB_INSTANCE_ID           => lv_job_instance_id,
            P_JOB_NAME                  => lv_job_name,
            P_JOB_AUDIT_LEVEL_ID        => NULL,
            P_COMMAND_NAME              => 'SQL',
            P_COMMAND_TYPE              => 'TRUNCATE',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            p_err_code := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      ---------------------------------------------------TRUNCATE STAGING TABLES SUBPARTITION-----------------------------------------------------
      lv_error_location := 2.5;
      p_mod_trunc := p_mod + 1;
      SCDH_REPORTING.REPORTING_UTILITIES.TRUNC_TABLE_SUBPARTITION (
         'SCDH_REPORTING',
         'OM_BACKLOG_HEADER_STG',
         'REGION_' || P_REGION,
         'REGION_' || P_REGION || '_MOD_' || p_mod_trunc,
         VAR_err_code,
         VAR_ERR_MSG);
      SCDH_REPORTING.REPORTING_UTILITIES.TRUNC_TABLE_SUBPARTITION (
         'SCDH_REPORTING',
         'OM_BACKLOG_DETAIL_STG',
         'REGION_' || P_REGION,
         'REGION_' || P_REGION || '_MOD_' || p_mod_trunc,
         VAR_err_code,
         VAR_ERR_MSG);

      IF VAR_err_code <> 0
      THEN
         p_err_code := VAR_err_code;
         p_err_mesg := VAR_ERR_MSG || 'Error Location:' || lv_error_location;
         RAISE exp_post;
      END IF;

      --------------------------------get fulf_region in package variables------------------------------------------------------------------
      IF p_region = 'AMER'
      THEN
         l_fulf_region1 := 'DAO';
         l_fulf_region2 := 'LATA';
      ELSE
         l_fulf_region1 := p_region;
         l_fulf_region2 := p_region;
      END IF;

      -----------------------------------------------Get STAGE,SUBSTAGE,backlog_facility CONDITIONS------------------------------------------
      lv_error_location := 2.6;
      SCDH_CODE.om_piece_part_bcklg_pkg.PRC_MDM_STG_SUBSTG_CONDITION (
         p_region,
         lv_error_code,
         lv_error_msg);

      IF lv_error_code <> 0
      THEN
         p_err_code := lv_error_code;
         p_err_mesg := lv_error_msg || 'Error Location:' || lv_error_location;
         RAISE exp_post;
      END IF;

      lv_error_location := 2.7;
      SCDH_CODE.om_piece_part_bcklg_pkg.prc_backlog_rule_setup (
         'BACKLOG_FACILITY',
         P_backlog_FCLTY_condition,
         p_region,
         lv_error_code,
         lv_error_msg);

      IF lv_error_code <> 0
      THEN
         p_err_code := lv_error_code;
         p_err_mesg := lv_error_msg || 'Error Location:' || lv_error_location;
         RAISE exp_post;
      END IF;

      lv_error_location := 2.8;
      SCDH_CODE.om_piece_part_bcklg_pkg.prc_backlog_rule_setup (
         'CONSUMPTION_STATUS',
         P_CON_STS_condition,
         p_region,
         lv_error_code,
         lv_error_msg);

      IF lv_error_code <> 0
      THEN
         p_err_code := lv_error_code;
         p_err_mesg := lv_error_msg || 'Error Location:' || lv_error_location;
         RAISE exp_post;
      END IF;

      lv_error_location := 2.9;
      SCDH_CODE.om_piece_part_bcklg_pkg.PRC_BKLG_FCLTY_CON_STS_CASE (
         P_backlog_FCLTY_condition,
         P_CON_STS_condition,
         p_mod,
         lv_error_code,
         lv_error_msg);                                                     --

      IF lv_error_code <> 0
      THEN
         p_err_code := lv_error_code;
         p_err_mesg := lv_error_msg || 'Error Location:' || lv_error_location;
         RAISE exp_post;
      END IF;

      -------------------------------------------------------------------------------------------------------------------------------
      IF cur_get_order%ISOPEN
      THEN
         CLOSE cur_get_order;
      END IF;

      IF cur_delete_sync%ISOPEN
      THEN
         CLOSE cur_delete_sync;
      END IF;

      ------------------------------------------get oms_source for oms_ordhdr and ibu_id flag table------------------------------------------------------------
      IF P_REGION = 'AMER'
      THEN
         V_OMS_SOURCE1 := 'OMS_AI';
         V_OMS_SOURCE2 := 'OMS_US';
      ELSIF P_REGION = 'APJ'
      THEN
         V_OMS_SOURCE1 := 'OMS_APJ';
         V_OMS_SOURCE2 := 'OMS_APJ';

         lv_error_location := 3.0;

         BEGIN
            SELECT propvalue
              INTO v_ibu_id_flag
              FROM FDL_SNOP_SCDHUB.MST_SYS_PROPS
             WHERE ID = 'PPB2-IBU_ID_FLAG';
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               v_ibu_id_flag := 'N';
         END;
      ELSIF P_REGION = 'EMEA'
      THEN
         V_OMS_SOURCE1 := 'OMS_EMEA';
         V_OMS_SOURCE2 := 'OMS_EMEA';
      END IF;

      -----------------------------------------------------------------------------------------------------------------------------------------------
      ---updates the status of previous step in PROCESS_JOB_DETAIL TABLE.
      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
            P_NUMBER_OF_ROWS_SELECTED   => NULL,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'Truncation and Stage-sub-stage condition calculation completed..',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            P_ERR_CODE := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      --DBMS_OUTPUT.PUT_LINE('IN PKG1');
      --------------------------------------Make the entry in log table for deleting source record------------------------------------------------------------
      lv_error_location := 3.1;

      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
            P_STEP_NAME                 => 'SOFT DELETE SOURCE DELETED RECORDS',
            P_JOB_AUDIT_MESSAGE         => 'Start soft Delete... ',
            P_NUMBER_OF_ROWS_SELECTED   => NULL,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_IS_BASE_TABLE             => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'update started..',
            P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
            P_JOB_INSTANCE_ID           => lv_job_instance_id,
            P_JOB_NAME                  => lv_job_name,
            P_JOB_AUDIT_LEVEL_ID        => NULL,
            P_COMMAND_NAME              => 'SQL',
            P_COMMAND_TYPE              => 'UPDATE',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            p_err_code := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      ------------------------SOFT DELETE ROWS TO SYNC OM_BACKLOG_HEADER AND BACKLOG HEADER TABLES----------------------------------------------
      IF P_INIT_FLAG != 'Y'
      THEN
                  --  Soft Delete only for incremental load
         OPEN cur_delete_sync (p_region, p_mod, 'HOURLY');                  --

         LOOP
            FETCH cur_delete_sync
               BULK COLLECT INTO t_delete_sync_tab
               LIMIT lv_limit2;

            EXIT WHEN t_delete_sync_tab.COUNT = 0;

            FORALL m IN 1 .. t_delete_sync_tab.COUNT
               UPDATE SCDH_REPORTING.OM_BACKLOG_HEADER
                  SET SYS_ENT_STATE = 'DELETED',
                      job_time = 'DELETED',
                      sys_last_modified_date = SYSTIMESTAMP
                WHERE     ORDER_NUM = t_delete_sync_tab (m).ORDER_NUMBER
                      AND PROD_ORDER_NUM =
                             t_delete_sync_tab (m).PROD_ORDER_NUM
                      AND BU_ID = t_delete_sync_tab (m).BU_ID
                      AND TIE_NUMBER = t_delete_sync_tab (m).TIE_NUMBER
                      AND REGION_CODE = p_region
                      AND JOB_TIME = 'HOURLY';

            rec_cnt_header := rec_cnt_header + SQL%ROWCOUNT;

            FORALL m IN 1 .. t_delete_sync_tab.COUNT
               UPDATE SCDH_REPORTING.OM_BACKLOG_DETAIL
                  SET SYS_ENT_STATE = 'DELETED',
                      job_time = 'DELETED',
                      sys_last_modified_date = SYSTIMESTAMP
                WHERE     ORDER_NUM = t_delete_sync_tab (m).ORDER_NUMBER
                      AND PROD_ORDER_NUM =
                             t_delete_sync_tab (m).PROD_ORDER_NUM
                      AND BU_ID = t_delete_sync_tab (m).BU_ID
                      AND TIE_NUMBER = t_delete_sync_tab (m).TIE_NUMBER
                      AND REGION_CODE = p_region
                      AND JOB_TIME = 'HOURLY';

            COMMIT;
         END LOOP;

         CLOSE cur_delete_sync;

      END IF;

      lv_error_location := 3.2;

      -----------------------------------------------------------------------------------------------------------------------------------------------
      ---updates the status of previous step in PROCESS_JOB_DETAIL TABLE.
      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
            P_NUMBER_OF_ROWS_SELECTED   => NULL,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => rec_cnt_header,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'Soft delete done..',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            P_ERR_CODE := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      ----------------------------------------------SOFT DELETE DONE-----------------------------------------------------------------------
      ------------------------------------------------Start insertion in stg tables-----------------------------------------------------
      --DBMS_OUTPUT.PUT_LINE('IN header');
      lv_error_location := 3.3;

      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
            P_STEP_NAME                 => 'INSERT',
            P_JOB_AUDIT_MESSAGE         =>    'INSERTING INTO OM BACKLOG HEADER STG FOR - '
                                           || p_region
                                           || '_'
                                           || p_mod,
            P_NUMBER_OF_ROWS_SELECTED   => NULL,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_IS_BASE_TABLE             => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'Insertion started..',
            P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
            P_JOB_INSTANCE_ID           => lv_job_instance_id,
            P_JOB_NAME                  => lv_job_name,
            P_JOB_AUDIT_LEVEL_ID        => NULL,
            P_COMMAND_NAME              => 'SQL',
            P_COMMAND_TYPE              => 'INSERT',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            p_err_code := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      ------------------------OPEN CURSOR TO GET THE MODIFIED ORDERS AFTER LAST SYNC UP DATE--------------------------------

      IF P_INIT_FLAG = 'Y'
      THEN                           --  If Date is NULL consider Initial Load
         OPEN cur_get_order_init (p_region, p_mod);                         --
      ELSE
         IF p_region = 'APJ'
         THEN
            --DBMS_OUTPUT.PUT_LINE('IN header CUR');
            OPEN cur_get_order_apj (p_region,
                                    p_start,
                                    p_end,
                                    V_OMS_SOURCE1,
                                    V_OMS_SOURCE2,
                                    p_mod);
         ELSE
            OPEN cur_get_order (p_region,
                                p_start,
                                p_end,
                                V_OMS_SOURCE1,
                                V_OMS_SOURCE2,
                                p_mod);
         END IF;
      END IF;

      LOOP
         IF P_INIT_FLAG = 'Y'
         THEN
            FETCH cur_get_order_init
               BULK COLLECT INTO t_get_order_tab
               LIMIT lv_limit2;
         ELSE
            IF p_region = 'APJ'
            THEN
               --DBMS_OUTPUT.PUT_LINE('IN header FETCH');
               FETCH cur_get_order_apj
                  BULK COLLECT INTO t_get_order_tab
                  LIMIT lv_limit2;

               DBMS_OUTPUT.put_line (
                     'No. of records in t_get_order_tab collection for '
                  || p_region
                  || ' is : '
                  || t_get_order_tab.COUNT);

               FETCH cur_get_order_apj
                  BULK COLLECT INTO t_cur_get_order_apj
                  LIMIT lv_limit2;

               DBMS_OUTPUT.put_line (
                     'No. of records in t_get_order_tab collection for '
                  || p_region
                  || ' is : '
                  || t_get_order_tab.COUNT);
            ELSE
               FETCH cur_get_order
                  BULK COLLECT INTO t_get_order_tab
                  LIMIT lv_limit2;

               DBMS_OUTPUT.put_line (
                     'No. of records in t_get_order_tab collection for '
                  || p_region
                  || '  is : '
                  || t_get_order_tab.COUNT);
            END IF;
         END IF;

         EXIT WHEN t_get_order_tab.COUNT = 0;
         ----------------------------MOVE DATA TO HEADER STAG TABLE ------------------------------------------
         lv_error_location := 3.4;
	==================================================================================================
         IF p_region = 'APJ'
         THEN
            --DBMS_OUTPUT.PUT_LINE('IN header apj');
            INSERT INTO SCDH_REPORTING.OM_BACKLOG_HEADER_STG (
                           PROD_ORDER_NUM,
                           PROD_ORDER_LINE,
                           ORDER_NUM,
                           BU_ID,
                           REGION_CODE,
                           TIE_NUMBER,
                           PO_NUM,
                           JOB_TIME,
                           QUOTE_NUMBER,
                           OMS_DOMS_STATUS,
                           DOMS_STATUS,
                           CYCLE_STAGE_CODE,
                           CYCLE_SUBSTAGE_CODE,
                           CYCLE_DATE,
                           SSC_NAME,
                           SS_TYPE_NAME,
                           MERGE_TYPE,
                           ORDER_TYPE,
                           BACKLOG_TYPE,
                           SOURCE_LOCAL_CHANNEL_CODE,
                           ORDER_DATE,
                           CANCEL_DATE,
                           IS_RETAIL,
                           SHIP_BY_DATE,
                           MUST_ARRIVE_BY_DATE,
                           CFI_FLAG,
                           TP_FACILITY,
                           SHIP_TO_FACILITY,
                           PLANNED_MERGE_FACILITY,
                           CUSTOMER_PICK_UP_FLAG,
                           SHIP_METHOD_CODE,
                           SHIP_MODE_CODE,
                           SHIP_CODE,
                           DIRECT_SHIP_FLAG,
                           SYSTEM_TYPE_CODE,
                           BASE_PROD_PROT_ID,
                           BASE_PROD_CODE,
                           FMLY_PARNT_NAME,
                           WORK_CENTER,
                           BASE_SKU,
                           base_type,
                           ccn,
                           CUSTOMER_NUM,
                           CUSTOMER_NAME,
                           RUSH_FLAG,
                           BUILD_TYPE,
                           ASN_LIFE_CYCLE_SUB_STATUS,
                           OFS_STATUS,
                           BACKLOG_ORDER_TIE_TYPE,
                           RUN_DATE,
                           SYS_SOURCE,
                           SYS_CREATION_DATE,
                           SYS_LAST_MODIFIED_DATE,
                           FULF_CHANNEL,
                           IBU_ID,
                           IP_DATE,
                           ITEM_QTY,
                           ORDER_PRIORITY,
                           PRIMARY_FC,
                           QTY_REQD,
                           RETAILER_NAME,
                           RPT_DOMS_STATUS,
                           SHIP_TO_COUNTRY,
                           SHIP_TO_STATE,
                           SUB_CHANNEL,
                           SYSTEM_QTY,
                           WORKCENTER,
                           sys_ent_state,
                           --Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3) BEGIN

                           --MABD_FLAG,
                           DP_ENABLED_FLAG,
                           ORIGINAL_RELEASE_DATE,
                           REVISED_RELEASE_DATE,
                           V_P_FLAG,
                           ESD,
                           EST_DELIVERY_DATE,
                           --Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3) END
                           SALESREP_NAME,
                           MOD_NUM,
                           --Changes to include FGA Direct Ship Orders starts
                           FGA_DIRECT_SHIP_FLAG
                           --Changes to include FGA Direct Ship Orders Ends
                           )
               (SELECT /*+ index(bh BKLG_HDR_IDX) use_nl (bh so po pol om_hd) */
                      BH.PROD_ORDER_NUMBER,
                       BH.PROD_ORDER_LINE,
                       BH.ORDER_NUMBER,
                       BH.BUID BU_ID,
                       BH.REGION_CODE REGION_CODE,
                       BH.TIE_NUMBER TIE_NUMBER,
                       NVL (SO.PURCHASE_ORDER_NUM, 'UNK') PO_NUM,
                       'HOURLY' JOB_TIME,
                       'UNK',
                       OM_HD.CURRENT_STATUS_CODE OMS_DOMS_STATUS,
                       BH.DOMS_STATUS DOMS_STATUS,
                       'UNK',
                       'UNK',
                       CASE
                          WHEN OM_HD.CURRENT_STATUS_CODE = 'OPR'
                          THEN
                             OM_HD.ORDER_DATE
                          WHEN OM_HD.CURRENT_STATUS_CODE = 'IP'
                          THEN
                             OM_HD.IN_PROD_DATETIME
                          WHEN OM_HD.CURRENT_STATUS_CODE = 'PP'
                          THEN
                             OM_HD.PEND_PROD_DATETIME
                          WHEN OM_HD.CURRENT_STATUS_CODE = 'MN'
                          THEN
                             OM_HD.SHIPPED_DATETIME
                          WHEN OM_HD.CURRENT_STATUS_CODE = 'IN'
                          THEN
                             OM_HD.INVOICE_DATE
                          ELSE
                             OM_HD.CURRENT_STATUS_DATETIME
                       END
                          CYCLE_DATE,
                       BH.SSC_NAME SSC_NAME,
                       'UNK' SS_TYPE_NAME,
                       NVL (SO.MERGE_TYPE, PO.MERGE_TYPE) MERGE_TYPE,
                       OM_HD.ORDER_TYPE_CODE ORDER_TYPE,
                       'BACKLOG_TYPE',
                       CASE -----------Local Channel for EMEA and APJ. AMER is fetched from get_ai_doms_ord function ------------------
                          WHEN BH.REGION_CODE = 'EMEA'
                          THEN
                             DECODE (so.buid,
                                     '5458', so.sales_channel_code,
                                     SUBSTR (so.sales_channel_code, 4))
                          WHEN BH.REGION_CODE = 'APJ'
                          THEN
                             NVL (
                                SUBSTR (so.cost_center,
                                        INSTR (so.cost_center, '_') + 1),
                                SUBSTR (
                                   so.sales_channel_code,
                                   INSTR (so.sales_channel_code, '_') + 1))
                          WHEN BH.REGION_CODE = 'AMER'
                          THEN
                             DECODE (so.buid,
                                     '11', so.company_num,
                                     NVL (so.customer_class, so.company_num))
                       END
                          SOURCE_LOCAL_CHANNEL_CODE,
                       NVL (SO.ORDER_DATE, BH.order_date) ORDER_DATE,
                       OM_HD.cancel_date CANCEL_DATE,
                       PO.IS_RETAIL IS_RETAIL,
                       BH.SHIP_BY_DATE SHIP_BY_DATE,
                       SO.MUST_ARRIVE_BY_DATE,
                       CASE WHEN POL.SI_NUMBER <> '0' THEN 'Y' ELSE 'N' END
                          CFI_FLAG,
                       BH.TP_FACILITY TP_FACILITY,
                       NVL (BH.SHIP_TO_FACILITY, po.ship_to_facility)
                          SHIP_TO_FACILITY,
                       BH.Ship_To_Facility PLANNED_MERGE_FACILITY,
                       fn_get_pickup_flag (BH.SHIP_CODE)
                          CUSTOMER_PICK_UP_FLAG,
                       'UNK',
                       NVL (SO.SHIP_MODE, PO.SHIP_MODE) SHIP_MODE_CODE,
                       BH.SHIP_CODE SHIP_CODE,
                       NULL,
                       'SYSTEM_TYPE_CODE',
                       NULL,
                       NULL,
                       NULL,
                       POL.WORK_CENTER WORK_CENTER,
                       BH.BASE_SKU,
                       BH.base_type,
                       BH.CCN,
                       SO.CUSTOMER_NUM CUSTOMER_NUM,
                       SO.CUSTOMER_NAME CUSTOMER_NAME,
                       OM_HD.Rush_Flag RUSH_FLAG,
                       PO.BUILD_TYPE BUILD_TYPE,
                       NULL,
                       BH.OFS_STATUS OFS_STATUS,
                       NULL, -- BACKLOG_ORDER_TIE_TYPE
                       SYSTIMESTAMP,
                       'FDL',
                       SYSTIMESTAMP,
                       SYSTIMESTAMP,
                       BH.FULF_CHANNEL,
                       BH.IBU_ID,
                       BH.IP_DATE,
                       BH.ITEM_QTY,
                       BH.ORDER_PRIORITY,
                       BH.PRIMARY_FC,
                       BH.QTY_REQD,
                       BH.RETAILER_NAME,
                       BH.RPT_DOMS_STATUS,
                       BH.SHIP_TO_COUNTRY,
                       BH.SHIP_TO_STATE,
                       BH.SUB_CHANNEL,
                       BH.SYSTEM_QTY,
                       BH.WORKCENTER,
                       'ACTIVE',
                       --Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3) Changes begin
                      /*CASE
                          WHEN NVL (TRUNC (SO.MUST_ARRIVE_BY_DATE), TO_DATE ('1/1/0001', 'DD/MM/YYYY')) < TO_DATE ('1/1/2000', 'DD/MM/YYYY')
                          THEN
                             'N'
                          -- WHEN TO_DATE ('1/1/1900', 'DD/MM/YYYY') THEN 'N'
                          ELSE
                             'Y'
                       END
                          MABD_FLAG, */
                        --commented above code as part of Story#6741082  
                       'N',
                       NULL,
                       NULL,
                       NVL (VEL_PRECISON_FLAG, 'V'),
                       SO.ESD,
                       SO.EST_DELIVERY_DATE,
                       --Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3)e Changes end
                       OM_HD.SALESREP_NAME,
                       p_moD,
                       --Changes to include FGA Direct Ship Orders starts
                       DECODE(UPPER(SA.ATTRIBUTE_NAME), 'ISFGADIRECTSHIP',
                        SA.ATTRIBUTE_VALUE,
                         NULL)                         --Changes to include FGA Direct Ship Orders ends
                  FROM TABLE (t_get_order_tab) T_ORDER
                       INNER JOIN SCDH_FULFILLMENT.BACKLOG_HEADER BH
                          ON T_ORDER.COLUMN_VALUE = BH.ORDER_NUMBER
                       LEFT JOIN SCDH_FULFILLMENT.SALES_ORDER SO
                          ON     BH.ORDER_NUMBER = SO.SALES_ORDER_ID
                             AND BH.REGION_CODE = SO.REGION_CODE
                             AND BH.BUID =
                                    (SELECT DECODE (
                                               v_ibu_id_flag,
                                               'Y', NVL (
                                                       (SELECT DISTINCT BU_ID
                                                          FROM Scdh_master.MST_IBU_BUID_XREF
                                                         WHERE IBU_ID =
                                                                  SO.BUID),
                                                       so.buid),
                                               SO.BUID)
                                       FROM DUAL)
                       LEFT JOIN SCDH_FULFILLMENT.PROD_ORDER PO
                          ON (    PO.PROD_ORDER_NUM = BH.PROD_ORDER_NUMBER
                              AND PO.REGION_CODE = BH.REGION_CODE)
                       LEFT JOIN SCDH_FULFILLMENT.PROD_ORDER_LINE POL
                          ON (    BH.PROD_ORDER_NUMBER = POL.PROD_ORDER_NUM
                              AND BH.REGION_CODE = POL.REGION_CODE
                              AND BH.PROD_ORDER_LINE = POL.LINE_NUM)
                       LEFT JOIN SCDH_FULFILLMENT.OMS_ORDHDR OM_HD
                          ON (    OM_HD.ORDER_NUM = BH.ORDER_NUMBER
                              AND OM_HD.BU_ID = BH.BUID
                              AND OM_HD.OMS_SOURCE IN (V_OMS_SOURCE1,
                                                       V_OMS_SOURCE2))
                       --Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3) Changes Begin
                       LEFT OUTER JOIN SCDH_FULFILLMENT.MFG_ORD_BKLG_VP BVP
                          ON     bh.order_number = BVP.ORD_NUM
                             AND bh.buid = BVP.BU_ID
                             AND TRUNC (bh.TIE_NUMBER) =
                                    TRUNC (BVP.ORD_TIE_NUM)
                             AND bh.region_code = BVP.region
                        --Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3) Changes End
                        -- Changes to include FGA Direct Ship Orders STARTS
                        LEFT OUTER JOIN SCDH_FULFILLMENT.SO_ATTRIBUTE SA
                            ON SO.SALES_ORDER_REF = SA.SALES_ORDER_REF
                            AND UPPER(SA.ATTRIBUTE_NAME) = 'ISFGADIRECTSHIP'
                        -- Changes to include FGA Direct Ship Orders ENDS

                 WHERE     BH.REGION_CODE = p_region --AND ORDER_NUMBER = p_order_num
                       AND BH.RPT_DOMS_STATUS <> 'OPR')
                    LOG ERRORS INTO SCDH_AUDIT.ERR_OM_BACKLOG_DETAILS
                           ('INSERT HEADER_STG-ACTIVE')
                           REJECT LIMIT UNLIMITED;


            --Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3) Changes Begin
            lv_error_location := 3.5;


            rec_cnt_header := rec_cnt_header + SQL%ROWCOUNT;
            DBMS_OUTPUT.put_line (
                  'No. of records inserted into  stg for  '
               || p_region
               || '  region is : '
               || rec_cnt_header);
         --Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3) end
         ELSE
            DBMS_OUTPUT.put_line ('came into else for other regions');

            INSERT INTO SCDH_REPORTING.OM_BACKLOG_HEADER_STG (                   -------------------------------------KKKKKKK
                           PROD_ORDER_NUM,
                           PROD_ORDER_LINE,
                           ORDER_NUM,
                           BU_ID,
                           REGION_CODE,
                           TIE_NUMBER,
                           PO_NUM,
                           JOB_TIME,
                           QUOTE_NUMBER,
                           OMS_DOMS_STATUS,
                           DOMS_STATUS,
                           CYCLE_STAGE_CODE,
                           CYCLE_SUBSTAGE_CODE,
                           CYCLE_DATE,
                           SSC_NAME,
                           SS_TYPE_NAME,
                           MERGE_TYPE,
                           ORDER_TYPE,
                           BACKLOG_TYPE,
                           SOURCE_LOCAL_CHANNEL_CODE,
                           ORDER_DATE,
                           CANCEL_DATE,
                           IS_RETAIL,
                           SHIP_BY_DATE,
                           MUST_ARRIVE_BY_DATE,
                           CFI_FLAG,
                           TP_FACILITY,
                           SHIP_TO_FACILITY,
                           PLANNED_MERGE_FACILITY,
                           CUSTOMER_PICK_UP_FLAG,
                           SHIP_METHOD_CODE,
                           SHIP_MODE_CODE,
                           SHIP_CODE,
                           DIRECT_SHIP_FLAG,
                           SYSTEM_TYPE_CODE,
                           BASE_PROD_PROT_ID,
                           BASE_PROD_CODE,
                           FMLY_PARNT_NAME,
                           WORK_CENTER,
                           BASE_SKU,
                           base_type,
                           ccn,
                           CUSTOMER_NUM,
                           CUSTOMER_NAME,
                           RUSH_FLAG,
                           BUILD_TYPE,
                           ASN_LIFE_CYCLE_SUB_STATUS,
                           OFS_STATUS,
                           BACKLOG_ORDER_TIE_TYPE,
                           RUN_DATE,
                           SYS_SOURCE,
                           SYS_CREATION_DATE,
                           SYS_LAST_MODIFIED_DATE,
                           FULF_CHANNEL,
                           IBU_ID,
                           IP_DATE,
                           ITEM_QTY,
                           ORDER_PRIORITY,
                           PRIMARY_FC,
                           QTY_REQD,
                           RETAILER_NAME,
                           RPT_DOMS_STATUS,
                           SHIP_TO_COUNTRY,
                           SHIP_TO_STATE,
                           SUB_CHANNEL,
                           SYSTEM_QTY,
                           WORKCENTER,
                           sys_ent_state,
                           -- Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3) begin
                           MABD_FLAG,
                           DP_ENABLED_FLAG,
                           ORIGINAL_RELEASE_DATE,
                           REVISED_RELEASE_DATE,
                           V_P_FLAG,
                           ESD,
                           EST_DELIVERY_DATE,
                           -- Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3) end
                           LOCAL_ORDER_TYPE,
                           SALESREP_NAME,
                           MOD_NUM,
                           --Changes to include FGA Direct Ship Orders starts
                           FGA_DIRECT_SHIP_FLAG,
                           asn_3pl,
                           asn_3pl_status,
                           om_order_type, --<RK004>
                           rsid    --<RK004>
                           --Changes to include FGA Direct Ship Orders ends
                           )
               (SELECT /*+ index(bh BKLG_HDR_IDX) use_nl (bh so po pol om_hd) */
                      BH.PROD_ORDER_NUMBER,
                       BH.PROD_ORDER_LINE,
                       BH.ORDER_NUMBER,
                       BH.BUID BU_ID,
                       BH.REGION_CODE REGION_CODE,
                       BH.TIE_NUMBER TIE_NUMBER,
                       NVL (SO.PURCHASE_ORDER_NUM, 'UNK') PO_NUM,
                       'HOURLY' JOB_TIME,
                       'UNK',
                       OM_HD.CURRENT_STATUS_CODE OMS_DOMS_STATUS,
                       BH.DOMS_STATUS DOMS_STATUS,
                       'UNK',
                       'UNK',
                       CASE
                          WHEN OM_HD.CURRENT_STATUS_CODE = 'OPR'
                          THEN
                             OM_HD.ORDER_DATE
                          WHEN OM_HD.CURRENT_STATUS_CODE = 'IP'
                          THEN
                             OM_HD.IN_PROD_DATETIME
                          WHEN OM_HD.CURRENT_STATUS_CODE = 'PP'
                          THEN
                             OM_HD.PEND_PROD_DATETIME
                          WHEN OM_HD.CURRENT_STATUS_CODE = 'MN'
                          THEN
                             OM_HD.SHIPPED_DATETIME
                          WHEN OM_HD.CURRENT_STATUS_CODE = 'IN'
                          THEN
                             OM_HD.INVOICE_DATE
                          ELSE
                             OM_HD.CURRENT_STATUS_DATETIME
                       END
                          CYCLE_DATE,
                       BH.SSC_NAME SSC_NAME,
                       'UNK' SS_TYPE_NAME,
                       NVL (SO.MERGE_TYPE, PO.MERGE_TYPE) MERGE_TYPE,
                       OM_HD.ORDER_TYPE_CODE ORDER_TYPE,
                       'BACKLOG_TYPE',
                       CASE -----------Local Channel for EMEA and APJ. AMER is fetched from get_ai_doms_ord function ------------------
                          WHEN BH.REGION_CODE = 'EMEA'
                          THEN
                             DECODE (so.buid,
                                     '5458', so.sales_channel_code,
                                     SUBSTR (so.sales_channel_code, 4))
                          WHEN BH.REGION_CODE = 'APJ'
                          THEN
                             NVL (
                                SUBSTR (so.cost_center,
                                        INSTR (so.cost_center, '_') + 1),
                                SUBSTR (
                                   so.sales_channel_code,
                                   INSTR (so.sales_channel_code, '_') + 1))
                          WHEN BH.REGION_CODE = 'AMER'
                          THEN
                             DECODE (so.buid,
                                     '11', so.company_num,
                                     NVL (so.customer_class, so.company_num))
                       END
                          SOURCE_LOCAL_CHANNEL_CODE,
                       NVL (SO.ORDER_DATE, BH.order_date) ORDER_DATE,
                       OM_HD.cancel_date CANCEL_DATE,
                       PO.IS_RETAIL IS_RETAIL,
                       BH.SHIP_BY_DATE SHIP_BY_DATE,
                       SO.MUST_ARRIVE_BY_DATE,
                       CASE WHEN POL.SI_NUMBER <> '0' THEN 'Y' ELSE 'N' END
                          CFI_FLAG,
                       BH.TP_FACILITY TP_FACILITY,
                       NVL (BH.SHIP_TO_FACILITY, po.ship_to_facility)
                          SHIP_TO_FACILITY,
                       BH.Ship_To_Facility PLANNED_MERGE_FACILITY,
                       fn_get_pickup_flag (BH.SHIP_CODE)
                          CUSTOMER_PICK_UP_FLAG,
                       'UNK',
                       NVL (SO.SHIP_MODE, PO.SHIP_MODE) SHIP_MODE_CODE,
                       BH.SHIP_CODE SHIP_CODE,
                       NULL,
                       'SYSTEM_TYPE_CODE',
                       NULL,
                       NULL,
                       NULL,
                       POL.WORK_CENTER WORK_CENTER,
                       BH.BASE_SKU,
                       BH.base_type,
                       BH.CCN,
                       SO.CUSTOMER_NUM CUSTOMER_NUM,
                       SO.CUSTOMER_NAME CUSTOMER_NAME,
                       OM_HD.Rush_Flag RUSH_FLAG,
                       PO.BUILD_TYPE BUILD_TYPE,
                       NULL,
                       BH.OFS_STATUS OFS_STATUS,
                       NULL,
                       SYSTIMESTAMP,
                       'FDL',
                       SYSTIMESTAMP,
                       SYSTIMESTAMP,
                       BH.FULF_CHANNEL,
                       BH.IBU_ID,
                       BH.IP_DATE,
                       BH.ITEM_QTY,
                       BH.ORDER_PRIORITY,
                       BH.PRIMARY_FC,
                       BH.QTY_REQD,
                       BH.RETAILER_NAME,
                       BH.RPT_DOMS_STATUS,
                       BH.SHIP_TO_COUNTRY,
                       BH.SHIP_TO_STATE,
                       BH.SUB_CHANNEL,
                       BH.SYSTEM_QTY,
                       BH.WORKCENTER,
                       'ACTIVE',
                       --Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3) begin
                       CASE
                          WHEN NVL (TRUNC (SO.MUST_ARRIVE_BY_DATE),
                                    TO_DATE ('1/1/0001', 'DD/MM/YYYY')) <
                                  TO_DATE ('1/1/2000', 'DD/MM/YYYY')
                          THEN
                             'N'
                          -- WHEN TO_DATE ('1/1/1900', 'DD/MM/YYYY') THEN 'N'
                          ELSE
                             'Y'
                       END
                          MABD_FLAG,
                       'N',
                       NULL,
                       NULL,
                       NVL (VEL_PRECISON_FLAG, 'V'),
                       SO.ESD,
                       SO.EST_DELIVERY_DATE,
                       --Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3) end
                       CASE
                          WHEN     BH.REGION_CODE = 'EMEA'
                               AND OM_HD.LOCAL_ORDER_TYPE LIKE '%Preshipment'
                          THEN
                             'PRESHIPMENT'
                          ELSE
                             'UNK'
                       END
                          AS LOCAL_ORDER_TYPE,
                       OM_HD.SALESREP_NAME,
                       p_mod,
                       --Changes to include FGA Direct Ship Orders starts
                       DECODE(UPPER(SA.ATTRIBUTE_NAME), 'ISFGADIRECTSHIP', SA.ATTRIBUTE_VALUE,
                        NULL),
                        NULL,
                        NULL,
                         om_order_type, --<RK004>
                        rsid    --<RK004>
                       --Changes to include FGA Direct Ship Orders ends
                  FROM TABLE (t_get_order_tab) T_ORDER
                       INNER JOIN SCDH_FULFILLMENT.BACKLOG_HEADER BH
                          ON T_ORDER.COLUMN_VALUE = BH.ORDER_NUMBER
                       LEFT JOIN SCDH_FULFILLMENT.SALES_ORDER SO
                          ON     BH.ORDER_NUMBER = SO.SALES_ORDER_ID
                             AND BH.REGION_CODE = SO.REGION_CODE
                             AND BH.BUID = SO.BUID
                       LEFT JOIN SCDH_FULFILLMENT.PROD_ORDER PO
                          ON (    PO.PROD_ORDER_NUM = BH.PROD_ORDER_NUMBER
                              AND PO.REGION_CODE = BH.REGION_CODE)
                       LEFT JOIN SCDH_FULFILLMENT.PROD_ORDER_LINE POL
                          ON (    BH.PROD_ORDER_NUMBER = POL.PROD_ORDER_NUM
                              AND BH.REGION_CODE = POL.REGION_CODE
                              AND BH.PROD_ORDER_LINE = POL.LINE_NUM)
                       LEFT JOIN SCDH_FULFILLMENT.OMS_ORDHDR OM_HD
                          ON (    OM_HD.ORDER_NUM = BH.ORDER_NUMBER
                              AND OM_HD.BU_ID = BH.BUID
                              AND OM_HD.OMS_SOURCE IN (V_OMS_SOURCE1,
                                                       V_OMS_SOURCE2))
                       --Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3) Changes Begin
                       LEFT JOIN SCDH_FULFILLMENT.MFG_ORD_BKLG_VP BVP
                          ON     bh.order_number = BVP.ORD_NUM
                             AND bh.buid = BVP.BU_ID
                             AND TRUNC (bh.TIE_NUMBER) =
                                    TRUNC (BVP.ORD_TIE_NUM)
                             AND bh.region_code = BVP.region
                 --Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3) Changes End
                 -- Changes to include FGA Direct Ship Orders STARTS
                        LEFT OUTER JOIN SCDH_FULFILLMENT.SO_ATTRIBUTE SA
                            ON SO.SALES_ORDER_REF = SA.SALES_ORDER_REF
                            AND UPPER(SA.ATTRIBUTE_NAME) = 'ISFGADIRECTSHIP'
                 -- Changes to include FGA Direct Ship Orders ENDS
                 WHERE     BH.REGION_CODE = p_region --AND ORDER_NUMBER = p_order_num
                       AND BH.RPT_DOMS_STATUS <> 'OPR')
                    LOG ERRORS INTO SCDH_AUDIT.ERR_OM_BACKLOG_DETAILS
                           ('INSERT HEADER_STG-ACTIVE')
                           REJECT LIMIT UNLIMITED;

            --             DBMS_OUTPUT.put_line ('order number is ' ||T_ORDER.COLUMN_VALUE );

            --            DBMS_OUTPUT.put_line ('No of records inserted' || SQL%ROWCOUNT);

            --            rec_cnt_header := rec_cnt_header + SQL%ROWCOUNT;
            DBMS_OUTPUT.put_line (
                  'No. of records inserted into  stg for  '
               || p_region
               || '  region is : '
               || SQL%ROWCOUNT);

            COMMIT;
         END IF;



         IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
               P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
               P_NUMBER_OF_ROWS_INSERTED   => lv_insert_rec,
               P_NUMBER_OF_ROWS_UPDATED    => NULL,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'INSERTION DONE FOR OM BACKLOG HEADER STG',
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);

            IF lv_error_code <> 0
            THEN
               P_ERR_CODE := lv_error_code;
               p_err_mesg :=
                  lv_error_msg || 'Error Location:' || lv_error_location;
               RAISE exp_post;
            END IF;
         END IF;

         -------------------------------------OPEN CURSOR FOR FETCHING DATA FOR POPULATION OF OM_BACKLOG_DETAIL----------------------------
         --DBMS_OUTPUT.PUT_LINE('IN detail');
         IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
               P_STEP_NAME                 => 'INSERT',
               P_JOB_AUDIT_MESSAGE         =>    'INSERTING INTO OM BACKLOG DETAIL STG FOR - '
                                              || p_region
                                              || '_'
                                              || p_mod,
               P_NUMBER_OF_ROWS_SELECTED   => NULL,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => NULL,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_IS_BASE_TABLE             => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'Insertion started..',
               P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
               P_JOB_INSTANCE_ID           => lv_job_instance_id,
               P_JOB_NAME                  => lv_job_name,
               P_JOB_AUDIT_LEVEL_ID        => NULL,
               P_COMMAND_NAME              => 'SQL',
               P_COMMAND_TYPE              => 'INSERT',
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);

            IF lv_error_code <> 0
            THEN
               p_err_code := lv_error_code;
               p_err_mesg :=
                  lv_error_msg || 'Error Location:' || lv_error_location;
               RAISE exp_post;
            END IF;
         END IF;

         lv_insert_rec := 0;
         lv_selcted_rec := 0;
         ---------------------------MOVE COLLECTION OF DETAIL RECORDS INTO STAG TABLE-----------------------------------------------------------
         lv_error_location := 3.6;

         DBMS_OUTPUT.PUT_LINE ('IN detail before insert');

         INSERT INTO scdh_reporting.OM_BACKLOG_DETAIL_STG ( -------------------------------------------------PPPPPPPP
                        PROD_ORDER_NUM,
                        PROD_ORDER_LINE,
                        ORDER_NUM,
                        BU_ID,
                        REGION_CODE,
                        TIE_NUMBER,
                        JOB_TIME,
                        SPAMS_FACILITY,
                        OVERPACK_FACILITY,
                        KITTING_FACILITY,
                        BOXING_FACILITY,
                        FAST_TRACK_CONFIG_ID,
                        ISSUE_CODE,
                        FGA_ID,
                        SKU_NUM,
                        POS_APOS,
                        TIE_GROUP,
                        MOD_PART_NUMBER,
                        PART_NUMBER,
                        CONSUMPTION_FACILITY,
                        CONSUMPTION_STATUS,
                        BACKLOG_FACILITY,
                        EMBEDDED_SS_FLAG,
                        PART_QTY,
                        QTY_STARTED,
                        released_dmd,
                        unreleased_dmd,
                        FUTURE_DMD,
                        RETURN_ON_ASN,
                        PART_CONSUMPTION_FACILITY,
                        BASE_TYPE,
                        PART_TYPE_CODE,
                        is_fga_sku,
                        box_code,
                        consumed_dmd,
                        dmd_loc_facility,
                        dms_flag,
                        dtl_seq_num,
                        ibu_id,
                        item_category,
                        net_dmd,
                        part_type_reason_code,
                        qty_extended,
                        qty_reqd,
                        summary_dmd,
                        sys_ent_state,
                        RUN_DATE,
                        SYS_SOURCE,
                        SYS_CREATION_DATE,
                        SYS_LAST_MODIFIED_DATE,
                        demand_supply_region_code,
                        MOD_NUM,
                        IL_ITEM_TYPE,
                        IL_RETURN_ON_ASN,
                        I_DESCRIPTION)
            (SELECT bd.PROD_ORDER_NUMBER,
                    bd.PROD_ORDER_LINE,
                    bd.ORDER_NUMBER,
                    bd.BUID,
                    bd.REGION_CODE,
                    bd.TIE_NUMBER,
                    'HOURLY',
                    po.SPAMS_FACILITY,
                    po.OVERPACK_FACILITY,
                    po.KITTING_FACILITY,
                    po.BOXING_FACILITY,
                    NULL,
                    bd.ISSUE_CODE,
                    NULL,
                    bd.SKU,
                    NULL,
                    NULL,
                    bd.MOD_PART_NUMBER,
                    bd.PART_NUMBER,
                    bd.CONSUMPTION_FACILITY,
                    'UNCONSUMED',
                    bd.CONSUMPTION_FACILITY,
                    'UNK',
                    bd.summary_dmd,
                    bd.QTY_STARTED,
                    bd.released_dmd,
                    bd.unreleased_dmd,
                    bd.FUTURE_DMD,
                    NULL,
                    bd.CONSUMPTION_FACILITY,
                    NULL,
                    bd.PART_TYPE_CODE,
                    bd.is_fga_sku,
                    bd.box_code,
                    bd.consumed_dmd,
                    bd.dmd_loc_facility,
                    bd.dms_flag,
                    bd.dtl_seq_num,
                    bd.ibu_id,
                    bd.item_category,
                    bd.net_dmd,
                    bd.part_type_reason_code,
                    bd.qty_extended,
                    bd.qty_reqd,
                    bd.summary_dmd,
                    'ACTIVE',
                    SYSTIMESTAMP,
                    'FDL',
                    SYSTIMESTAMP,
                    SYSTIMESTAMP,
                    fn_demand_supply_reg (bd.consumption_facility)
                       demand_supply_region_code,
                    p_mod,
                    NULL,
                    NULL,
                    NULL
               FROM TABLE (t_get_order_tab) T_ORDER
                    INNER JOIN scdh_reporting.OM_BACKLOG_HEADER_STG bh
                       ON T_ORDER.COLUMN_VALUE = BH.ORDER_NUM
                    JOIN scdh_fulfillment.backlog_detail bd
                       ON     bh.region_code = bd.region_code
                          AND bh.order_num = bd.order_number
                          AND bh.prod_order_num = bd.prod_order_numBer
                          AND bh.tie_number = bd.tie_number
                          AND bh.bu_id = bd.buid
                    LEFT JOIN scdh_fulfillment.prod_order po
                       ON     po.prod_order_num = bd.prod_order_number
                          AND po.region_code = bd.region_code
              WHERE     bd.region_code = p_region
                    AND bh.region_code = p_region
                    AND bh.mod_num = p_mod)
                 LOG ERRORS INTO SCDH_AUDIT.ERR_OM_BACKLOG_DETAILS
                        ('INSERT DETAIL_STG-ACTIVE')
                        REJECT LIMIT UNLIMITED;
          dbms_output.put_line('no of records inserted in detail stg:'||lv_insert_rec);
         lv_insert_rec := SQL%ROWCOUNT;
         lv_selcted_rec := SQL%ROWCOUNT;

         COMMIT;

         ---updates the status of previous step in PROCESS_JOB_DETAIL TABLE.
         IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
               P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
               P_NUMBER_OF_ROWS_INSERTED   => lv_insert_rec,
               P_NUMBER_OF_ROWS_UPDATED    => NULL,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'INSERTION DONE FOR OM BACKLOG DETAIL STG',
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);

            IF lv_error_code <> 0
            THEN
               P_ERR_CODE := lv_error_code;
               p_err_mesg :=
                  lv_error_msg || 'Error Location:' || lv_error_location;
               RAISE exp_post;
            END IF;
         END IF;
      END LOOP;

      IF cur_get_order%ISOPEN
      THEN
         CLOSE cur_get_order;
      ELSIF cur_get_order_init%ISOPEN
      THEN
         CLOSE cur_get_order_init;
      ELSIF cur_get_order_apj%ISOPEN
      THEN
         CLOSE cur_get_order_apj;
      ELSIF cur_get_FDD_APJ%ISOPEN
      THEN
         CLOSE cur_get_FDD_APJ;
      ELSIF cur_get_FDD%ISOPEN
      THEN
         CLOSE cur_get_FDD;
      END IF;                         ---------------END OF cur_get_order LOOP

      DBMS_OUTPUT.PUT_LINE ('POR detail before  insert');
      ---------------------------------------------------------------------------------------------------------------------------------------------------
      SCDH_CODE.om_piece_part_bcklg_pkg.prc_populating_header_dtl_opr (
         p_region,
         p_start,
         p_end,
         p_mod,
         V_OMS_SOURCE1,
         V_OMS_SOURCE2,
         P_INIT_FLAG);

      DBMS_OUTPUT.PUT_LINE ('OPR detail after  insert');

      ---------------dp enabled flag logic begins------------

      lv_updated_rec := 0;
      lv_selcted_rec := 0;
      lv_error_location := 3.7;

IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
            P_STEP_NAME                 => 'UPDATE',
            P_JOB_AUDIT_MESSAGE         => 'UPDATE STG FOR CUR_GET_SO_LEAD FOR - '
                                           || p_region
                                           || '_'
                                           || p_mod,
            P_NUMBER_OF_ROWS_SELECTED   => NULL,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_IS_BASE_TABLE             => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'UPDATE started..',
            P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
            P_JOB_INSTANCE_ID           => lv_job_instance_id,
            P_JOB_NAME                  => lv_job_name,
            P_JOB_AUDIT_LEVEL_ID        => NULL,
            P_COMMAND_NAME              => 'SQL',
            P_COMMAND_TYPE              => 'UPDATE',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            p_err_code := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
         RAISE exp_post;
         END IF;
      END IF;


BEGIN
   IF cur_get_FDD_APJ%ISOPEN
   THEN
      CLOSE cur_get_FDD_APJ;
   END IF;

   OPEN cur_get_FDD_apj (p_region, p_mod);

   LOOP
      FETCH cur_get_FDD_apj
         BULK COLLECT INTO t_FDD_APJ
         LIMIT lv_limit2;

      EXIT WHEN t_FDD_APJ.COUNT = 0;

      FORALL M IN 1 .. t_FDD_APJ.COUNT SAVE EXCEPTIONS
         UPDATE SCDH_REPORTING.OM_BACKLOG_HEADER_STG
            SET FDD_FLAG =
                   CASE
                      WHEN ( t_FDD_APJ (m).FDD_DATE IS NOT NULL
                            AND TRUNC(t_FDD_APJ (m).FDD_DATE) <> TO_DATE('1/1/0001','MM/DD/YYYY')
                            AND t_FDD_APJ (m).FDD_FLAG = 'Y')
                      THEN
                         'Y'
                      ELSE
                         t_FDD_APJ (m).FDD_FLAG
                   END,
                FDD_DATE = t_FDD_APJ (m).FDD_DATE
          WHERE     ORDER_NUM = t_FDD_APJ (m).ORDER_NUM
                AND ROWID = t_FDD_APJ (m).HDR_ROW
                AND MOD_NUM = P_MOD;


      lv_updated_rec := lv_updated_rec + SQL%ROWCOUNT;
      lv_selcted_rec := lv_selcted_rec + t_FDD_APJ.COUNT;
   END LOOP;

   COMMIT;
-----------------------------------------
EXCEPTION
   WHEN OTHERS
   THEN
      FOR i IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
      LOOP
         IF t_FDD_APJ.EXISTS (SQL%BULK_EXCEPTIONS (i).ERROR_INDEX)
         THEN
            lv_indx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
            lv_loop_cnt := lv_loop_cnt + 1;
            t_excp_po (lv_loop_cnt).ORA_ERR_ROWID$ :=
               t_FDD_APJ (lv_indx).HDR_ROW;
            t_excp_po (lv_loop_cnt).ORDER_NUM := t_FDD_APJ (lv_indx).ORDER_NUM;

            t_excp_po (lv_loop_cnt).sys_err_code :=
               SQL%BULK_EXCEPTIONS (i).ERROR_CODE;
            t_excp_po (lv_loop_cnt).sys_err_mesg :=
                  SQLERRM (SQL%BULK_EXCEPTIONS (i).ERROR_CODE * -1)
               || 'ERR LOC '
               || lv_error_location;
         END IF;
      END LOOP;
END;


      IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
               P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => lv_updated_rec,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'UPDATION DONE FOR CUR_GET_SO_LEAD',
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);

            IF lv_error_code <> 0
            THEN
               P_ERR_CODE := lv_error_code;
               p_err_mesg :=
                  lv_error_msg || 'Error Location:' || lv_error_location;
               RAISE exp_post;
            END IF;
         END IF;


      --- dp enabled fla logic ends----------

      IF P_REGION = 'APJ'
      THEN

      lv_updated_rec := 0;
      lv_selcted_rec := 0;
      lv_error_location := 3.8;

    IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
            P_STEP_NAME                 => 'UPDATE',
            P_JOB_AUDIT_MESSAGE         => 'UPDATE STG FOR CUR_GET_FDD_APJ FOR - '
                                           || p_region
                                           || '_'
                                           || p_mod,
            P_NUMBER_OF_ROWS_SELECTED   => NULL,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_IS_BASE_TABLE             => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'Update started..',
            P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
            P_JOB_INSTANCE_ID           => lv_job_instance_id,
            P_JOB_NAME                  => lv_job_name,
            P_JOB_AUDIT_LEVEL_ID        => NULL,
            P_COMMAND_NAME              => 'SQL',
            P_COMMAND_TYPE              => 'UPDATE',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            p_err_code := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;


         BEGIN
            IF cur_get_FDD_APJ%ISOPEN
            THEN
               CLOSE cur_get_FDD_APJ;
            END IF;

            OPEN cur_get_FDD_apj (p_region, p_mod);

            LOOP
               FETCH cur_get_FDD_apj
                  BULK COLLECT INTO t_FDD_APJ
                  LIMIT lv_limit2;

               EXIT WHEN t_FDD_APJ.COUNT = 0;

               FORALL M IN 1 .. t_FDD_APJ.COUNT SAVE EXCEPTIONS
                  UPDATE SCDH_REPORTING.OM_BACKLOG_HEADER_STG
                     SET FDD_FLAG = case when 
        (t_FDD_APJ (m).FDD_DATE  IS NOT NULL
       AND t_FDD_APJ (m).FDD_DATE  <> '1/1/0001 12:00:00 AM' AND t_FDD_APJ (m).FDD_FLAG = 'Y') then 'Y' ELSE t_FDD_APJ (m).FDD_FLAG
                     END,
                         FDD_DATE = t_FDD_APJ (m).FDD_DATE
                   WHERE     ORDER_NUM = t_FDD_APJ (m).ORDER_NUM
                         AND ROWID = t_FDD_APJ (m).HDR_ROW
                         AND MOD_NUM = P_MOD;


                lv_updated_rec := lv_updated_rec + SQL%ROWCOUNT;
                lv_selcted_rec := lv_selcted_rec + t_FDD_APJ.COUNT;

            END LOOP;

            COMMIT;
         -----------------------------------------
         EXCEPTION
            WHEN OTHERS
            THEN
               FOR i IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
               LOOP
                  IF t_FDD_APJ.EXISTS (SQL%BULK_EXCEPTIONS (i).ERROR_INDEX)
                  THEN
                     lv_indx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
                     lv_loop_cnt := lv_loop_cnt + 1;
                     t_excp_po (lv_loop_cnt).ORA_ERR_ROWID$ :=
                        t_FDD_APJ (lv_indx).HDR_ROW;
                     t_excp_po (lv_loop_cnt).ORDER_NUM :=
                        t_FDD_APJ (lv_indx).ORDER_NUM;

                     t_excp_po (lv_loop_cnt).sys_err_code :=
                        SQL%BULK_EXCEPTIONS (i).ERROR_CODE;
                     t_excp_po (lv_loop_cnt).sys_err_mesg :=
                           SQLERRM (SQL%BULK_EXCEPTIONS (i).ERROR_CODE * -1)
                        || 'ERR LOC '
                        || lv_error_location;
                  END IF;
               END LOOP;
         END;


    IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
               P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => lv_updated_rec,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'UPDATION DONE FOR CUR_GET_FDD_APJ',
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);

            IF lv_error_code <> 0
            THEN
               P_ERR_CODE := lv_error_code;
               p_err_mesg :=
                  lv_error_msg || 'Error Location:' || lv_error_location;
               RAISE exp_post;
            END IF;
         END IF;

---------------------------------------------------------------------------------

    lv_updated_rec := 0;
    lv_selcted_rec := 0;
    lv_error_location := 3.9;

    IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
            P_STEP_NAME                 => 'UPDATE',
            P_JOB_AUDIT_MESSAGE         => 'UPDATE STG FOR CUR_MABD_Y FOR - '
                                           || p_region
                                           || '_'
                                           || p_mod,
            P_NUMBER_OF_ROWS_SELECTED   => NULL,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_IS_BASE_TABLE             => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'Update started..',
            P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
            P_JOB_INSTANCE_ID           => lv_job_instance_id,
            P_JOB_NAME                  => lv_job_name,
            P_JOB_AUDIT_LEVEL_ID        => NULL,
            P_COMMAND_NAME              => 'SQL',
            P_COMMAND_TYPE              => 'UPDATE',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            p_err_code := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;


         OPEN CUR_MABD_Y (P_REGION, P_MOD);

         LOOP
            FETCH CUR_MABD_Y
               BULK COLLECT INTO t_FDD_MABD_Y
               LIMIT lv_limit3;

            EXIT WHEN t_FDD_MABD_Y.COUNT = 0;

            BEGIN
               FORALL j IN t_FDD_MABD_Y.FIRST .. t_FDD_MABD_Y.LAST
                 SAVE EXCEPTIONS
                  UPDATE SCDH_REPORTING.OM_BACKLOG_HEADER_STG
                     --SET FDD_FLAG = 'N' --commented above code as part of Story#6741082
                     SET MABD_FLAG = 'Y'
                      WHERE     REGION_CODE = P_REGION
                         AND ROWID = t_FDD_MABD_Y (j).HDR_ROW;

               lv_updated_rec := lv_updated_rec + SQL%ROWCOUNT;
               lv_selcted_rec := lv_selcted_rec + t_FDD_MABD_Y.COUNT;

               COMMIT;


            EXCEPTION
               WHEN OTHERS
               THEN
                  FOR i IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
                  LOOP
                     IF t_FDD_MABD_Y.EXISTS (
                           SQL%BULK_EXCEPTIONS (i).ERROR_INDEX)
                     THEN
                        lv_indx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
                        lv_loop_cnt := lv_loop_cnt + 1;
                        t_excp_po (lv_loop_cnt).ORA_ERR_ROWID$ :=
                           t_FDD_MABD_Y (lv_indx).HDR_ROW;
                        t_excp_po (lv_loop_cnt).ORDER_NUM :=
                           t_FDD_MABD_Y (lv_indx).ORDER_NUM;

                        t_excp_po (lv_loop_cnt).sys_err_code :=
                           SQL%BULK_EXCEPTIONS (i).ERROR_CODE;
                        t_excp_po (lv_loop_cnt).sys_err_mesg :=
                              SQLERRM (
                                 SQL%BULK_EXCEPTIONS (i).ERROR_CODE * -1)
                           || 'ERR LOC '
                           || lv_error_location;
                     END IF;
                  END LOOP;
            END;
         END LOOP;

         CLOSE CUR_MABD_Y;


      IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
               P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => lv_updated_rec,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'UPDATION DONE FOR CUR_MABD_Y',
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);

            IF lv_error_code <> 0
            THEN
               P_ERR_CODE := lv_error_code;
               p_err_mesg :=
                  lv_error_msg || 'Error Location:' || lv_error_location;
               RAISE exp_post;
            END IF;
         END IF;
      END IF;


     --- lv_error_location := 1.15;

      ---updates the status of previous step in PROCESS_JOB_DETAIL TABLE.

       --update FDD_FLAG  update based upon OPR set of tables start <KR002>

       IF P_REGION  IN ('AMER', 'EMEA' )
            THEN

      lv_updated_rec := 0;
      lv_selcted_rec := 0;
      lv_error_location := 4.0;

    IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
            P_STEP_NAME                 => 'UPDATE',
            P_JOB_AUDIT_MESSAGE         => 'UPDATE STG FOR CUR_GET_FDD_OPR FOR - '
                                           || p_region
                                           || '_'
                                           || p_mod,
            P_NUMBER_OF_ROWS_SELECTED   => NULL,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_IS_BASE_TABLE             => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'Update started..',
            P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
            P_JOB_INSTANCE_ID           => lv_job_instance_id,
            P_JOB_NAME                  => lv_job_name,
            P_JOB_AUDIT_LEVEL_ID        => NULL,
            P_COMMAND_NAME              => 'SQL',
            P_COMMAND_TYPE              => 'UPDATE',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            p_err_code := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;


                 BEGIN
                    IF cur_get_FDD_OPR%ISOPEN
                    THEN
                       CLOSE cur_get_FDD_OPR;
                    END IF;

                    OPEN cur_get_FDD_OPR (p_region, p_mod);

                    LOOP
                       FETCH cur_get_FDD_OPR BULK COLLECT INTO t_FDD_OPR LIMIT lv_limit2;

                       EXIT WHEN t_FDD_OPR.COUNT = 0;

                       FORALL M IN 1 .. t_FDD_OPR.COUNT SAVE EXCEPTIONS
                          UPDATE SCDH_REPORTING.OM_BACKLOG_HEADER_STG
                             SET FDD_FLAG = 'Y'
                           WHERE     ORDER_NUM = t_FDD_OPR (m).ORDER_NUM
                                 AND REGION_CODE = P_REGION
--                                 AND ROWID = t_FDD_OPR (m).HDR_ROW
                                 AND MOD_NUM = P_MOD;

                    lv_updated_rec := lv_updated_rec + SQL%ROWCOUNT;
                    lv_selcted_rec := lv_selcted_rec + t_FDD_OPR.COUNT;

                    END LOOP;

                    COMMIT;
                 EXCEPTION
                    WHEN OTHERS
                    THEN
                       FOR i IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
                       LOOP
                          IF t_FDD.EXISTS (SQL%BULK_EXCEPTIONS (i).ERROR_INDEX)
                          THEN
                             lv_indx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
                             lv_loop_cnt := lv_loop_cnt + 1;
--                             t_excp_po (lv_loop_cnt).ORA_ERR_ROWID$ :=
--                                t_FDD (lv_indx).HDR_ROW;
                             t_excp_po (lv_loop_cnt).ORDER_NUM :=
                                t_FDD (lv_indx).ORDER_NUM;
                             t_excp_po (lv_loop_cnt).sys_err_code :=
                                SQL%BULK_EXCEPTIONS (i).ERROR_CODE;
                             t_excp_po (lv_loop_cnt).sys_err_mesg :=
                                   SQLERRM (SQL%BULK_EXCEPTIONS (i).ERROR_CODE * -1)
                                || 'ERR LOC '
                                || lv_error_location;
                          END IF;
                       END LOOP;
                 END;

        IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
               P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => lv_updated_rec,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'UPDATION DONE FOR CUR_GET_FDD_OPR',
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);

            IF lv_error_code <> 0
            THEN
               P_ERR_CODE := lv_error_code;
               p_err_mesg :=
                  lv_error_msg || 'Error Location:' || lv_error_location;
               RAISE exp_post;
            END IF;
         END IF;
      END IF;

      --update FDD_FLAG  update based upon OPR set of tables end <KR002>


    IF P_REGION = 'EMEA'
    THEN

      lv_updated_rec := 0;
      lv_selcted_rec := 0;
      lv_error_location := 4.1;

    IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
            P_STEP_NAME                 => 'UPDATE',
            P_JOB_AUDIT_MESSAGE         => 'UPDATE STG FOR CUR_GET_FDD FOR - '
                                           || p_region
                                           || '_'
                                           || p_mod,
            P_NUMBER_OF_ROWS_SELECTED   => NULL,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_IS_BASE_TABLE             => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'Update started..',
            P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
            P_JOB_INSTANCE_ID           => lv_job_instance_id,
            P_JOB_NAME                  => lv_job_name,
            P_JOB_AUDIT_LEVEL_ID        => NULL,
            P_COMMAND_NAME              => 'SQL',
            P_COMMAND_TYPE              => 'UPDATE',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
      THEN
            p_err_code := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

         BEGIN
            IF cur_get_FDD%ISOPEN
            THEN
               CLOSE cur_get_FDD;
            END IF;

            OPEN cur_get_FDD (p_region, p_mod);

            LOOP
               FETCH cur_get_FDD BULK COLLECT INTO t_FDD LIMIT lv_limit2;

               EXIT WHEN t_FDD.COUNT = 0;

               FORALL M IN 1 .. t_FDD.COUNT SAVE EXCEPTIONS
                  UPDATE SCDH_REPORTING.OM_BACKLOG_HEADER_STG
                     SET FDD_FLAG = 'Y'
                   WHERE     ORDER_NUM = t_FDD (m).ORDER_NUM
                         AND REGION_CODE = P_REGION
--                         AND ROWID = t_FDD (m).HDR_ROW
                         AND MOD_NUM = P_MOD;

                lv_updated_rec := lv_updated_rec + SQL%ROWCOUNT;
                lv_selcted_rec := lv_selcted_rec + t_FDD.COUNT;

            END LOOP;

            COMMIT;
         EXCEPTION
            WHEN OTHERS
            THEN
               FOR i IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
               LOOP
                  IF t_FDD.EXISTS (SQL%BULK_EXCEPTIONS (i).ERROR_INDEX)
                  THEN
                     lv_indx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
                     lv_loop_cnt := lv_loop_cnt + 1;
--                     t_excp_po (lv_loop_cnt).ORA_ERR_ROWID$ :=
--                        t_FDD (lv_indx).HDR_ROW;
                     t_excp_po (lv_loop_cnt).ORDER_NUM :=
                        t_FDD (lv_indx).ORDER_NUM;
                     t_excp_po (lv_loop_cnt).sys_err_code :=
                        SQL%BULK_EXCEPTIONS (i).ERROR_CODE;
                     t_excp_po (lv_loop_cnt).sys_err_mesg :=
                           SQLERRM (SQL%BULK_EXCEPTIONS (i).ERROR_CODE * -1)
                        || 'ERR LOC '
                        || lv_error_location;
                  END IF;
               END LOOP;
         END;


    IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
               P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => lv_updated_rec,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'UPDATION DONE FOR CUR_GET_FDD',
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);

            IF lv_error_code <> 0
            THEN
               P_ERR_CODE := lv_error_code;
               p_err_mesg :=
                  lv_error_msg || 'Error Location:' || lv_error_location;
               RAISE exp_post;
            END IF;
         END IF;

         COMMIT;

-----------------------------------------------------------------------------------
     lv_updated_rec := 0;
     lv_selcted_rec := 0;
     lv_error_location := 4.2;

    IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
            P_STEP_NAME                 => 'UPDATE',
            P_JOB_AUDIT_MESSAGE         =>  'UPDATE STG FOR CUR_MABD_Y FOR - '
                                           || p_region
                                           || '_'
                                           || p_mod,
            P_NUMBER_OF_ROWS_SELECTED   => NULL,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_IS_BASE_TABLE             => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'Update started..',
            P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
            P_JOB_INSTANCE_ID           => lv_job_instance_id,
            P_JOB_NAME                  => lv_job_name,
            P_JOB_AUDIT_LEVEL_ID        => NULL,
            P_COMMAND_NAME              => 'SQL',
            P_COMMAND_TYPE              => 'UPDATE',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            p_err_code := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;



         IF CUR_MABD_Y%ISOPEN
         THEN
            CLOSE CUR_MABD_Y;
         END IF;

         OPEN CUR_MABD_Y (P_REGION, P_MOD);

         LOOP
            FETCH CUR_MABD_Y
               BULK COLLECT INTO t_FDD_MABD_Y
               LIMIT lv_limit3;

            EXIT WHEN t_FDD_MABD_Y.COUNT = 0;

            BEGIN
               FORALL j IN t_FDD_MABD_Y.FIRST .. t_FDD_MABD_Y.LAST
                 SAVE EXCEPTIONS
                  UPDATE SCDH_REPORTING.OM_BACKLOG_HEADER_STG
                     SET FDD_FLAG = 'N'
                   WHERE     ROWID = t_FDD_MABD_Y (j).HDR_ROW
                         AND REGION_CODE = P_REGION
                         AND MOD_NUM = P_MOD;

                 lv_updated_rec := lv_updated_rec + SQL%ROWCOUNT;
                 lv_selcted_rec := lv_selcted_rec + t_FDD_MABD_Y.COUNT;

               COMMIT;


            EXCEPTION
               WHEN OTHERS
               THEN
                  FOR i IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
                  LOOP
                     IF t_FDD_MABD_Y.EXISTS (
                           SQL%BULK_EXCEPTIONS (i).ERROR_INDEX)
                     THEN
                        lv_indx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
                        lv_loop_cnt := lv_loop_cnt + 1;
                        t_excp_po (lv_loop_cnt).ORA_ERR_ROWID$ :=
                           t_FDD_MABD_Y (lv_indx).HDR_ROW;
                        t_excp_po (lv_loop_cnt).ORDER_NUM :=
                           t_FDD_MABD_Y (lv_indx).ORDER_NUM;

                        t_excp_po (lv_loop_cnt).sys_err_code :=
                           SQL%BULK_EXCEPTIONS (i).ERROR_CODE;
                        t_excp_po (lv_loop_cnt).sys_err_mesg :=
                              SQLERRM (
                                 SQL%BULK_EXCEPTIONS (i).ERROR_CODE * -1)
                           || 'ERR LOC '
                           || lv_error_location;
                     END IF;
                  END LOOP;
            END;
         END LOOP;

         CLOSE CUR_MABD_Y;

      IF lv_audit_log_yn = 'Y'
      THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
               P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => lv_updated_rec,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'UPDATION DONE FOR CUR_MABD_Y',
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);

            IF lv_error_code <> 0
            THEN
               P_ERR_CODE := lv_error_code;
               p_err_mesg :=
                  lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
            END IF;
          END IF;
      END IF;

      IF P_REGION = 'AMER'
      THEN

        lv_updated_rec := 0;
        lv_selcted_rec := 0;
        lv_error_location := 4.3;

    IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
            P_STEP_NAME                 => 'UPDATE',
            P_JOB_AUDIT_MESSAGE         => 'UPDATE STG FOR CUR_GET_FDD FOR - '
                                           || p_region
                                           || '_'
                                           || p_mod,
            P_NUMBER_OF_ROWS_SELECTED   => NULL,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_IS_BASE_TABLE             => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'Update started..',
            P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
            P_JOB_INSTANCE_ID           => lv_job_instance_id,
            P_JOB_NAME                  => lv_job_name,
            P_JOB_AUDIT_LEVEL_ID        => NULL,
            P_COMMAND_NAME              => 'SQL',
            P_COMMAND_TYPE              => 'UPDATE',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            p_err_code := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;


         BEGIN
            IF cur_get_FDD%ISOPEN
            THEN
               CLOSE cur_get_FDD;
            END IF;

            OPEN cur_get_FDD (p_region, p_mod);

            LOOP
               FETCH cur_get_FDD BULK COLLECT INTO t_FDD LIMIT lv_limit2;

               EXIT WHEN t_FDD.COUNT = 0;

               FORALL M IN 1 .. t_FDD.COUNT SAVE EXCEPTIONS
                  UPDATE SCDH_REPORTING.OM_BACKLOG_HEADER_STG
                     SET FDD_FLAG = 'Y'
                   WHERE     ORDER_NUM = t_FDD (m).ORDER_NUM
--                         AND ROWID = t_FDD (m).HDR_ROW
                         AND REGION_CODE = P_REGION
                         AND MOD_NUM = P_MOD;

                lv_updated_rec := lv_updated_rec + SQL%ROWCOUNT;
                lv_selcted_rec := lv_selcted_rec + t_FDD.COUNT;

            END LOOP;

            COMMIT;
         EXCEPTION
            WHEN OTHERS
            THEN
               FOR i IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
               LOOP
                  IF t_FDD.EXISTS (SQL%BULK_EXCEPTIONS (i).ERROR_INDEX)
                  THEN
                     lv_indx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
                     lv_loop_cnt := lv_loop_cnt + 1;
--                     t_excp_po (lv_loop_cnt).ORA_ERR_ROWID$ :=
--                        t_FDD (lv_indx).HDR_ROW;
                     t_excp_po (lv_loop_cnt).ORDER_NUM :=
                        t_FDD (lv_indx).ORDER_NUM;

                     t_excp_po (lv_loop_cnt).sys_err_code :=
                        SQL%BULK_EXCEPTIONS (i).ERROR_CODE;
                     t_excp_po (lv_loop_cnt).sys_err_mesg :=
                           SQLERRM (SQL%BULK_EXCEPTIONS (i).ERROR_CODE * -1)
                        || 'ERR LOC '
                        || lv_error_location;
                  END IF;
               END LOOP;
         END;

         COMMIT;


    IF lv_audit_log_yn = 'Y'
        THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
               P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => lv_updated_rec,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'UPDATION DONE FOR CUR_GET_FDD',
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);

            IF lv_error_code <> 0
            THEN
               P_ERR_CODE := lv_error_code;
               p_err_mesg :=
                  lv_error_msg || 'Error Location:' || lv_error_location;
               RAISE exp_post;
            END IF;
         END IF;



------------------------------------------------------------------------------------------------------------

         --- Pending--

    lv_updated_rec := 0;
    lv_selcted_rec := 0;
    lv_error_location := 4.4;

    IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
            P_STEP_NAME                 => 'UPDATE',
            P_JOB_AUDIT_MESSAGE         => 'UPDATE STG FOR CUR_FDD_MABD_AMER_Y1 FOR - '
                                           || p_region
                                           || '_'
                                           || p_mod,
            P_NUMBER_OF_ROWS_SELECTED   => NULL,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_IS_BASE_TABLE             => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'Update started..',
            P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
            P_JOB_INSTANCE_ID           => lv_job_instance_id,
            P_JOB_NAME                  => lv_job_name,
            P_JOB_AUDIT_LEVEL_ID        => NULL,
            P_COMMAND_NAME              => 'SQL',
            P_COMMAND_TYPE              => 'UPDATE',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            p_err_code := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;


         IF CUR_FDD_MABD_AMER_Y1%ISOPEN
         THEN
            CLOSE CUR_FDD_MABD_AMER_Y1;
         END IF;


         OPEN CUR_FDD_MABD_AMER_Y1 (P_REGION, P_MOD);

         LOOP
            FETCH CUR_FDD_MABD_AMER_Y1
               BULK COLLECT INTO t_FDD_MABD_AMER_Y1
               LIMIT lv_limit3;

            EXIT WHEN t_FDD_MABD_AMER_Y1.COUNT = 0;

            BEGIN
               FORALL j
                   IN t_FDD_MABD_AMER_Y1.FIRST .. t_FDD_MABD_AMER_Y1.LAST
                 SAVE EXCEPTIONS
                  UPDATE SCDH_REPORTING.OM_BACKLOG_HEADER_STG
                     SET MABD_FLAG = 'N'
                   WHERE     ROWID = t_FDD_MABD_AMER_Y1 (j).HDR_ROW
                         AND REGION_CODE = P_REGION
                         AND mod_num = p_mod;

                lv_updated_rec := lv_updated_rec + SQL%ROWCOUNT;
                lv_selcted_rec := lv_selcted_rec + t_FDD_MABD_AMER_Y1.COUNT;

               COMMIT;


            EXCEPTION
               WHEN OTHERS
               THEN
                  FOR i IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
                  LOOP
                     IF t_FDD_MABD_AMER_Y1.EXISTS (
                           SQL%BULK_EXCEPTIONS (i).ERROR_INDEX)
                     THEN
                        lv_indx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
                        lv_loop_cnt := lv_loop_cnt + 1;
                        t_excp_po (lv_loop_cnt).ORA_ERR_ROWID$ :=
                           t_FDD_MABD_AMER_Y1 (lv_indx).HDR_ROW;
                        t_excp_po (lv_loop_cnt).ORDER_NUM :=
                           t_FDD_MABD_AMER_Y1 (lv_indx).ORDER_NUM;

                        t_excp_po (lv_loop_cnt).sys_err_code :=
                           SQL%BULK_EXCEPTIONS (i).ERROR_CODE;
                        t_excp_po (lv_loop_cnt).sys_err_mesg :=
                              SQLERRM (
                                 SQL%BULK_EXCEPTIONS (i).ERROR_CODE * -1)
                           || 'ERR LOC '
                           || lv_error_location;
                     END IF;
                  END LOOP;
            END;
         END LOOP;

         CLOSE CUR_FDD_MABD_AMER_Y1;

         COMMIT;

        IF lv_audit_log_yn = 'Y'
        THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
               P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => lv_updated_rec,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'UPDATION DONE FOR CUR_FDD_MABD_AMER_Y1',
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);

            IF lv_error_code <> 0
            THEN
               P_ERR_CODE := lv_error_code;
               p_err_mesg :=
                  lv_error_msg || 'Error Location:' || lv_error_location;
               RAISE exp_post;
            END IF;
         END IF;

---------------------------------------------------------------------------------------------------

    lv_updated_rec := 0;
    lv_selcted_rec := 0;
    lv_error_location := 4.5;

    IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
            P_STEP_NAME                 => 'UPDATE',
            P_JOB_AUDIT_MESSAGE         => 'UPDATE STG FOR CUR_FDD_MABD_AMER_Y2 FOR - '
                                           || p_region
                                           || '_'
                                           || p_mod,
            P_NUMBER_OF_ROWS_SELECTED   => NULL,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_IS_BASE_TABLE             => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'Update started..',
            P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
            P_JOB_INSTANCE_ID           => lv_job_instance_id,
            P_JOB_NAME                  => lv_job_name,
            P_JOB_AUDIT_LEVEL_ID        => NULL,
            P_COMMAND_NAME              => 'SQL',
            P_COMMAND_TYPE              => 'UPDATE',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            p_err_code := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;


         IF CUR_FDD_MABD_AMER_Y2%ISOPEN
         THEN
            CLOSE CUR_FDD_MABD_AMER_Y2;
         END IF;

         OPEN CUR_FDD_MABD_AMER_Y2 (P_REGION, P_MOD);

         LOOP
            FETCH CUR_FDD_MABD_AMER_Y2
               BULK COLLECT INTO t_FDD_MABD_AMER_Y2
               LIMIT lv_limit3;

            EXIT WHEN t_FDD_MABD_AMER_Y2.COUNT = 0;

            BEGIN
               FORALL j
                   IN t_FDD_MABD_AMER_Y2.FIRST .. t_FDD_MABD_AMER_Y2.LAST
                 SAVE EXCEPTIONS
                  UPDATE SCDH_REPORTING.OM_BACKLOG_HEADER_STG
                     SET FDD_FLAG = 'N'
                   WHERE     ROWID = t_FDD_MABD_AMER_Y2 (j).HDR_ROW
                         AND REGION_CODE = P_REGION
                         AND mod_num = p_mod;

                    lv_updated_rec := lv_updated_rec + SQL%ROWCOUNT;
                    lv_selcted_rec := lv_selcted_rec + t_FDD_MABD_AMER_Y2.COUNT;

               COMMIT;


            EXCEPTION
               WHEN OTHERS
               THEN
                  FOR i IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
                  LOOP
                     IF t_FDD_MABD_AMER_Y2.EXISTS (
                           SQL%BULK_EXCEPTIONS (i).ERROR_INDEX)
                     THEN
                        lv_indx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
                        lv_loop_cnt := lv_loop_cnt + 1;
                        t_excp_po (lv_loop_cnt).ORA_ERR_ROWID$ :=
                           t_FDD_MABD_AMER_Y2 (lv_indx).HDR_ROW;
                        t_excp_po (lv_loop_cnt).ORDER_NUM :=
                           t_FDD_MABD_AMER_Y2 (lv_indx).ORDER_NUM;

                        t_excp_po (lv_loop_cnt).sys_err_code :=
                           SQL%BULK_EXCEPTIONS (i).ERROR_CODE;
                        t_excp_po (lv_loop_cnt).sys_err_mesg :=
                              SQLERRM (
                                 SQL%BULK_EXCEPTIONS (i).ERROR_CODE * -1)
                           || 'ERR LOC '
                           || lv_error_location;
                     END IF;
                  END LOOP;
            END;
         END LOOP;

         COMMIT;

         CLOSE CUR_FDD_MABD_AMER_Y2;

        IF lv_audit_log_yn = 'Y'
        THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
               P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => lv_updated_rec,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'UPDATION DONE FOR CUR_FDD_MABD_AMER_Y2',
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);

            IF lv_error_code <> 0
            THEN
               P_ERR_CODE := lv_error_code;
               p_err_mesg :=
                  lv_error_msg || 'Error Location:' || lv_error_location;
               RAISE exp_post;
            END IF;
         END IF;
      END IF;


      ------------------------------------------------Delete Failed Record---------------------------------------------------------
      ------------Delete from Header---------------------


        lv_deletd_rec := 0;
        lv_selcted_rec := 0;
        lv_error_location := 4.6;

        IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
               P_STEP_NAME                 => 'DELETE',
               P_JOB_AUDIT_MESSAGE         => 'DELETE BEGIN FOR STG and DETAIL - '
                                                || p_region
                                                || '_'
                                                || p_mod,
               P_NUMBER_OF_ROWS_SELECTED   => NULL,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => NULL,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_IS_BASE_TABLE             => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'DELETE started...',
               P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
               P_JOB_INSTANCE_ID           => lv_job_instance_id,
               P_JOB_NAME                  => lv_job_name,
               P_JOB_AUDIT_LEVEL_ID        => NULL,
               P_COMMAND_NAME              => 'SQL',
               P_COMMAND_TYPE              => 'DELETE',
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);

            IF lv_error_code <> 0
            THEN
               p_err_code := lv_error_code;
               p_err_mesg :=
                  lv_error_msg || 'Error Location:' || lv_error_location;
               RAISE exp_post;
            END IF;
         END IF;

      DELETE FROM SCDH_REPORTING.OM_BACKLOG_HEADER_STG OBH
            WHERE     EXISTS
                         (SELECT 1
                            FROM SCDH_AUDIT.ERR_OM_BACKLOG_DETAILS EOBD
                           WHERE     EOBD.ora_err_tag$ LIKE '%ACTIVE%'
                                 AND EOBD.PROD_ORDER_NUM = OBH.PROD_ORDER_NUM
                                 AND EOBD.ORDER_NUM = OBH.ORDER_NUM
                                 AND EOBD.BU_ID = OBH.BU_ID
                                 AND EOBD.REGION_CODE = OBH.REGION_CODE
                                 AND EOBD.JOB_TIME = OBH.JOB_TIME
                                 AND EOBD.MOD_NUM = OBH.MOD_NUM)
                  AND OBH.JOB_TIME = 'HOURLY'
                  AND OBH.REGION_CODE = p_region
                  AND OBH.MOD_NUM = p_mod;

        lv_error_location := 4.7;

      ------------Delete all entries from detail---------
      DELETE FROM SCDH_REPORTING.OM_BACKLOG_DETAIL_STG OBD
            WHERE     EXISTS
                         (SELECT 1
                            FROM SCDH_AUDIT.ERR_OM_BACKLOG_DETAILS EOBD
                           WHERE     EOBD.ora_err_tag$ LIKE '%ACTIVE%'
                                 AND EOBD.ORDER_NUM = OBD.ORDER_NUM
                                 AND EOBD.PROD_ORDER_NUM = OBD.PROD_ORDER_NUM
                                 AND EOBD.BU_ID = OBD.BU_ID
                                 AND EOBD.REGION_CODE = OBD.REGION_CODE
                                 AND EOBD.JOB_TIME = OBD.JOB_TIME
                                 AND EOBD.MOD_NUM = OBD.MOD_NUM)
                  AND OBD.JOB_TIME = 'HOURLY'
                  AND OBD.REGION_CODE = p_region
                  AND OBD.MOD_NUM = p_mod;

            lv_deletd_rec := lv_deletd_rec + SQL%ROWCOUNT;

      COMMIT;

      IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
               P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => NULL,
               P_NUMBER_OF_ROWS_DELETED    => lv_deletd_rec,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'DELETE DONE FOR STG AND DETAIL',
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);

            IF lv_error_code <> 0
            THEN
               P_ERR_CODE := lv_error_code;
               p_err_mesg :=
                  lv_error_msg || 'Error Location:' || lv_error_location;
               RAISE exp_post;
            END IF;
         END IF;



      -----------------------------------POPULATE REMAINING FIELDS FOR HEADER AND DETAIL-----------------------------------------------
      ----------------THESE FIELDS ARE BASED ON SOME LOGIC AND EXPENSIVE JOINS HENCE KEPT SEPERATE FROM MAIN LOGIC----------------------

      lv_error_location := 4.8;

      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
            P_STEP_NAME                 => 'UPDATE',
            P_JOB_AUDIT_MESSAGE         =>    'UPDATE DETAIL STG FOR CUR_ITEM_LOC FOR - '
                                           || p_region
                                           || '_'
                                           || p_mod,
            P_NUMBER_OF_ROWS_SELECTED   => NULL,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_IS_BASE_TABLE             => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'Update started..',
            P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
            P_JOB_INSTANCE_ID           => lv_job_instance_id,
            P_JOB_NAME                  => lv_job_name,
            P_JOB_AUDIT_LEVEL_ID        => NULL,
            P_COMMAND_NAME              => 'SQL',
            P_COMMAND_TYPE              => 'UPDATE',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            p_err_code := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      lv_insert_rec := 0;
      lv_selcted_rec := 0;

      OPEN CUR_ITEM_LOC (P_REGION, P_MOD);                                  --

      LOOP
         FETCH CUR_ITEM_LOC BULK COLLECT INTO t_item_loc LIMIT lv_limit4;

         EXIT WHEN t_item_loc.COUNT = 0;

         BEGIN
            FORALL j IN t_item_loc.FIRST .. t_item_loc.LAST SAVE EXCEPTIONS
               UPDATE SCDH_REPORTING.OM_BACKLOG_DETAIL_STG
                  SET IL_RETURN_ON_ASN = t_item_loc (j).return_on_asn,
                      IL_ITEM_TYPE = t_item_loc (j).ITEM_TYPE,
                      I_DESCRIPTION = t_item_loc (j).DESCRIPTION
                WHERE ROWID = t_item_loc (j).DTL_ROW;

            COMMIT;

            lv_insert_rec := lv_insert_rec + SQL%ROWCOUNT;
            lv_selcted_rec := lv_selcted_rec + t_item_loc.COUNT;
         EXCEPTION
            WHEN OTHERS
            THEN
               FOR i IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
               LOOP
                  IF t_item_loc.EXISTS (SQL%BULK_EXCEPTIONS (i).ERROR_INDEX)
                  THEN
                     lv_indx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
                     lv_loop_cnt := lv_loop_cnt + 1;
                     t_excp_po (lv_loop_cnt).ORA_ERR_ROWID$ :=
                        t_item_loc (lv_indx).DTL_ROW;
                     t_excp_po (lv_loop_cnt).ORDER_NUM :=
                        t_item_loc (lv_indx).ORDER_NUM;
                     t_excp_po (lv_loop_cnt).PROD_ORDER_NUM :=
                        t_item_loc (lv_indx).PROD_ORDER_NUM;
                     t_excp_po (lv_loop_cnt).TIE_NUMBER :=
                        t_item_loc (lv_indx).TIE_NUMBER;
                     t_excp_po (lv_loop_cnt).MOD_PART_NUMBER :=
                        t_item_loc (lv_indx).MOD_PART_NUMBER;
                     t_excp_po (lv_loop_cnt).PART_NUMBER :=
                        t_item_loc (lv_indx).PART_NUMBER;
                     t_excp_po (lv_loop_cnt).DTL_SEQ_NUM :=
                        t_item_loc (lv_indx).DTL_SEQ_NUM;
                     t_excp_po (lv_loop_cnt).sys_err_code :=
                        SQL%BULK_EXCEPTIONS (i).ERROR_CODE;
                     t_excp_po (lv_loop_cnt).sys_err_mesg :=
                           SQLERRM (SQL%BULK_EXCEPTIONS (i).ERROR_CODE * -1)
                        || 'ERR LOC '
                        || lv_error_location;
                  END IF;
               END LOOP;
         END;
      END LOOP;

      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
            P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => lv_insert_rec,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'UPDATE DONE FOR CUR_ITEM_LOC',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            P_ERR_CODE := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      lv_error_location := 4.9;

      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
            P_STEP_NAME                 => 'UPDATE',
            P_JOB_AUDIT_MESSAGE         =>    'UPDATE HEADER AND DETAIL STG FOR CUR_SYS_MAKE_PICK FOR - '
                                           || p_region
                                           || '_'
                                           || p_mod,
            P_NUMBER_OF_ROWS_SELECTED   => NULL,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_IS_BASE_TABLE             => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'Update started..',
            P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
            P_JOB_INSTANCE_ID           => lv_job_instance_id,
            P_JOB_NAME                  => lv_job_name,
            P_JOB_AUDIT_LEVEL_ID        => NULL,
            P_COMMAND_NAME              => 'SQL',
            P_COMMAND_TYPE              => 'UPDATE',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            p_err_code := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      lv_insert_rec := 0;
      lv_selcted_rec := 0;

      OPEN CUR_SYS_MAKE_PICK (P_REGION, p_mod);                             --

      LOOP
         FETCH CUR_SYS_MAKE_PICK
            BULK COLLECT INTO t_sys_make_pick
            LIMIT lv_limit4;

         EXIT WHEN t_sys_make_pick.COUNT = 0;

         BEGIN
            FORALL i IN t_sys_make_pick.FIRST .. t_sys_make_pick.LAST
              SAVE EXCEPTIONS
               UPDATE SCDH_REPORTING.OM_BACKLOG_HEADER_STG
                  SET SS_TYPE_NAME = t_sys_make_pick (i).ss_type_name,
                      backlog_type = t_sys_make_pick (i).backlog_type,
                      DIRECT_SHIP_FLAG = t_sys_make_pick (i).DIRECT_SHIP_FLAG,
                      SYSTEM_TYPE_CODE = t_sys_make_pick (i).SYSTEM_TYPE_CODE,
                      SUB_RGN_CDE = t_sys_make_pick (i).SUB_RGN_CDE,
                      BASE_PROD_PROT_ID =
                         t_sys_make_pick (i).BASE_PROD_PROT_ID,
                      BASE_PROD_CODE = t_sys_make_pick (i).BASE_PROD_CODE,
                      FMLY_PARNT_NAME = t_sys_make_pick (i).FMLY_PARNT_NAME,
                      PICK_MAKE = t_sys_make_pick (i).PICK_MAKE,
                      FACILITY_TYPE = t_sys_make_pick (i).FACILITY_TYPE
                WHERE ROWID = t_sys_make_pick (i).H_ROWID;

            lv_insert_rec := lv_insert_rec + SQL%ROWCOUNT;
            lv_selcted_rec := lv_selcted_rec + t_sys_make_pick.COUNT;
            COMMIT;
         EXCEPTION
            WHEN OTHERS
            THEN
               FOR i IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
               LOOP
                  IF t_sys_make_pick.EXISTS (
                        SQL%BULK_EXCEPTIONS (i).ERROR_INDEX)
                  THEN
                     lv_indx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
                     lv_loop_cnt := lv_loop_cnt + 1;
                     t_excp_po (lv_loop_cnt).PROD_ORDER_NUM :=
                        t_sys_make_pick (lv_indx).PROD_ORDER_NUM;
                     t_excp_po (lv_loop_cnt).TIE_NUMBER :=
                        t_sys_make_pick (lv_indx).TIE_NUMBER;
                     t_excp_po (lv_loop_cnt).ORDER_NUM :=
                        t_sys_make_pick (lv_indx).ORDER_NUM;
                     t_excp_po (lv_loop_cnt).BU_ID :=
                        t_sys_make_pick (lv_indx).BU_ID;
                     t_excp_po (lv_loop_cnt).REGION_CODE :=
                        t_sys_make_pick (lv_indx).REGION_CODE;
                     t_excp_po (lv_loop_cnt).JOB_TIME :=
                        t_sys_make_pick (lv_indx).JOB_TIME;
                     t_excp_po (lv_loop_cnt).sys_err_code :=
                        SQL%BULK_EXCEPTIONS (i).ERROR_CODE;
                     t_excp_po (lv_loop_cnt).sys_err_mesg :=
                           SQLERRM (SQL%BULK_EXCEPTIONS (i).ERROR_CODE * -1)
                        || 'ERR LOC '
                        || lv_error_location
                        || 'Header';
                  END IF;
               END LOOP;
         END;

         BEGIN
            FORALL i IN t_sys_make_pick.FIRST .. t_sys_make_pick.LAST
              SAVE EXCEPTIONS
               UPDATE SCDH_REPORTING.OM_BACKLOG_DETAIL_STG
                  SET POS_APOS = t_sys_make_pick (i).POS_APOS,
                      TIE_GROUP = t_sys_make_pick (i).TIE_GROUP,
                      RETURN_ON_ASN = t_sys_make_pick (i).RETURN_ON_ASN,
                      IS_SUPRESSED = t_sys_make_pick (i).is_supressed,
                      FGA_ID = t_sys_make_pick (i).fga_id,
                      BASE_TYPE = t_sys_make_pick (i).base_type,
                      FAST_TRACK_CONFIG_ID =
                         t_sys_make_pick (i).FASTTRACK_CONFIG_ID,
                      job_time = t_sys_make_pick (i).job_time
                WHERE ROWID = t_sys_make_pick (i).D_ROWID;

            COMMIT;
         EXCEPTION
            WHEN OTHERS
            THEN
               FOR i IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
               LOOP
                  IF t_sys_make_pick.EXISTS (
                        SQL%BULK_EXCEPTIONS (i).ERROR_INDEX)
                  THEN
                     lv_indx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
                     lv_loop_cnt := lv_loop_cnt + 1;
                     t_excp_po (lv_loop_cnt).PROD_ORDER_NUM :=
                        t_sys_make_pick (lv_indx).PROD_ORDER_NUM;
                     t_excp_po (lv_loop_cnt).TIE_NUMBER :=
                        t_sys_make_pick (lv_indx).TIE_NUMBER;
                     t_excp_po (lv_loop_cnt).ORDER_NUM :=
                        t_sys_make_pick (lv_indx).ORDER_NUM;
                     t_excp_po (lv_loop_cnt).BU_ID :=
                        t_sys_make_pick (lv_indx).BU_ID;
                     t_excp_po (lv_loop_cnt).REGION_CODE :=
                        t_sys_make_pick (lv_indx).REGION_CODE;
                     t_excp_po (lv_loop_cnt).JOB_TIME :=
                        t_sys_make_pick (lv_indx).JOB_TIME;
                     t_excp_po (lv_loop_cnt).MOD_PART_NUMBER :=
                        t_sys_make_pick (lv_indx).MOD_PART_NUMBER;
                     t_excp_po (lv_loop_cnt).PART_NUMBER :=
                        t_sys_make_pick (lv_indx).PART_NUMBER;
                     t_excp_po (lv_loop_cnt).DTL_SEQ_NUM :=
                        t_sys_make_pick (lv_indx).DTL_SEQ_NUM;
                     t_excp_po (lv_loop_cnt).sys_err_code :=
                        SQL%BULK_EXCEPTIONS (i).ERROR_CODE;
                     t_excp_po (lv_loop_cnt).sys_err_mesg :=
                           SQLERRM (SQL%BULK_EXCEPTIONS (i).ERROR_CODE * -1)
                        || 'ERR LOC '
                        || lv_error_location
                        || 'Detail';
                  END IF;
               END LOOP;
         END;
      END LOOP;

      ---updates the status of previous step in PROCESS_JOB_DETAIL TABLE.
      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
            P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => lv_insert_rec,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'UPDATE DONE FOR CUR_SYS_MAKE_PICK',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            P_ERR_CODE := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      -----------------------------------------------------------------------------------------------------------------------------------------------
      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
            P_STEP_NAME                 => 'UPDATE',
            P_JOB_AUDIT_MESSAGE         =>    'UPDATE DETAIL STG FOR CUR_SYS_MAKE_UNK FOR - '
                                           || p_region
                                           || '_'
                                           || p_mod,
            P_NUMBER_OF_ROWS_SELECTED   => NULL,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_IS_BASE_TABLE             => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'Update started..',
            P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
            P_JOB_INSTANCE_ID           => lv_job_instance_id,
            P_JOB_NAME                  => lv_job_name,
            P_JOB_AUDIT_LEVEL_ID        => NULL,
            P_COMMAND_NAME              => 'SQL',
            P_COMMAND_TYPE              => 'UPDATE',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            p_err_code := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      lv_insert_rec := 0;
      lv_selcted_rec := 0;

      OPEN CUR_SYS_MAKE_UNK (P_REGION, P_MOD);                              --

      LOOP
         FETCH CUR_SYS_MAKE_UNK
            BULK COLLECT INTO t_sys_make_unk
            LIMIT lv_limit4;

         EXIT WHEN t_sys_make_unk.COUNT = 0;

         BEGIN
            FORALL i IN t_sys_make_unk.FIRST .. t_sys_make_unk.LAST
              SAVE EXCEPTIONS
               UPDATE SCDH_REPORTING.OM_BACKLOG_HEADER_STG
                  SET PICK_MAKE = t_sys_make_unk (i).PICK_MAKE
                WHERE ROWID = t_sys_make_unk (i).ROWID;

            lv_insert_rec := lv_insert_rec + SQL%ROWCOUNT;
            lv_selcted_rec := lv_selcted_rec + t_sys_make_unk.COUNT;

            COMMIT;
         EXCEPTION
            WHEN OTHERS
            THEN
               FOR i IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
               LOOP
                  IF t_sys_make_unk.EXISTS (
                        SQL%BULK_EXCEPTIONS (i).ERROR_INDEX)
                  THEN
                     lv_indx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
                     lv_loop_cnt := lv_loop_cnt + 1;
                     t_excp_po (lv_loop_cnt).PROD_ORDER_NUM :=
                        t_sys_make_unk (lv_indx).PROD_ORDER_NUM;
                     t_excp_po (lv_loop_cnt).TIE_NUMBER :=
                        t_sys_make_unk (lv_indx).TIE_NUMBER;
                     t_excp_po (lv_loop_cnt).ORDER_NUM :=
                        t_sys_make_unk (lv_indx).ORDER_NUM;
                     t_excp_po (lv_loop_cnt).BU_ID :=
                        t_sys_make_unk (lv_indx).BU_ID;
                     t_excp_po (lv_loop_cnt).REGION_CODE :=
                        t_sys_make_unk (lv_indx).REGION_CODE;
                     t_excp_po (lv_loop_cnt).JOB_TIME :=
                        t_sys_make_unk (lv_indx).JOB_TIME;
                     t_excp_po (lv_loop_cnt).sys_err_code :=
                        SQL%BULK_EXCEPTIONS (i).ERROR_CODE;
                     t_excp_po (lv_loop_cnt).sys_err_mesg :=
                           SQLERRM (SQL%BULK_EXCEPTIONS (i).ERROR_CODE * -1)
                        || 'ERR LOC '
                        || lv_error_location;
                  END IF;
               END LOOP;
         END;
      END LOOP;

      CLOSE CUR_SYS_MAKE_UNK;

      ---updates the status of previous step in PROCESS_JOB_DETAIL TABLE.
      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
            P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => lv_insert_rec,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'UPDATE DONE FOR CUR_SYS_MAKE_UNK',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            P_ERR_CODE := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      -------Date:27-Apr-2014 Comment:This cursor was put due to base_prod_code,BASE_PROD_PROT_ID,FMLY_PARNT_CODE coming as NULL
      lv_error_location := 5.0;

      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
            P_STEP_NAME                 => 'UPDATE',
            P_JOB_AUDIT_MESSAGE         =>    'UPDATE HEADER AND DETAIL STG FOR CUR_PROT_ID_NULL FOR - '
                                           || p_region
                                           || '_'
                                           || p_mod,
            P_NUMBER_OF_ROWS_SELECTED   => NULL,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_IS_BASE_TABLE             => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'Update started..',
            P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
            P_JOB_INSTANCE_ID           => lv_job_instance_id,
            P_JOB_NAME                  => lv_job_name,
            P_JOB_AUDIT_LEVEL_ID        => NULL,
            P_COMMAND_NAME              => 'SQL',
            P_COMMAND_TYPE              => 'UPDATE',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            p_err_code := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      lv_insert_rec := 0;
      lv_selcted_rec := 0;

      OPEN CUR_PROT_ID_NULL (P_REGION, P_MOD);                              --

      LOOP
         FETCH CUR_PROT_ID_NULL
            BULK COLLECT INTO t_prot_id_null
            LIMIT lv_limit4;

         EXIT WHEN t_prot_id_null.COUNT = 0;

         BEGIN
            FORALL i IN t_prot_id_null.FIRST .. t_prot_id_null.LAST
              SAVE EXCEPTIONS
               UPDATE scdh_reporting.OM_BACKLOG_HEADER_STG bh
                  SET base_prod_code = t_prot_id_null (i).base_prod_code,
                      BASE_PROD_PROT_ID = t_prot_id_null (i).BASE_PROD_PROT_ID,
                      FMLY_PARNT_NAME = t_prot_id_null (i).FMLY_PARNT_CODE
                WHERE ROWID = t_prot_id_null (i).ROWID;

            lv_insert_rec := lv_insert_rec + SQL%ROWCOUNT;
            lv_selcted_rec := lv_selcted_rec + t_prot_id_null.COUNT;

            COMMIT;
         EXCEPTION
            WHEN OTHERS
            THEN
               FOR i IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
               LOOP
                  IF t_prot_id_null.EXISTS (
                        SQL%BULK_EXCEPTIONS (i).ERROR_INDEX)
                  THEN
                     lv_indx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
                     lv_loop_cnt := lv_loop_cnt + 1;
                     t_excp_po (lv_loop_cnt).PROD_ORDER_NUM :=
                        t_prot_id_null (lv_indx).PROD_ORDER_NUM;
                     t_excp_po (lv_loop_cnt).TIE_NUMBER :=
                        t_prot_id_null (lv_indx).TIE_NUMBER;
                     t_excp_po (lv_loop_cnt).ORDER_NUM :=
                        t_prot_id_null (lv_indx).ORDER_NUM;
                     t_excp_po (lv_loop_cnt).BU_ID :=
                        t_prot_id_null (lv_indx).BU_ID;
                     t_excp_po (lv_loop_cnt).REGION_CODE :=
                        t_prot_id_null (lv_indx).REGION_CODE;
                     t_excp_po (lv_loop_cnt).sys_err_code :=
                        SQL%BULK_EXCEPTIONS (i).ERROR_CODE;
                     t_excp_po (lv_loop_cnt).sys_err_mesg :=
                           SQLERRM (SQL%BULK_EXCEPTIONS (i).ERROR_CODE * -1)
                        || 'ERR LOC '
                        || lv_error_location;
                  END IF;
               END LOOP;
         END;
      END LOOP;

      CLOSE CUR_PROT_ID_NULL;

      ---updates the status of previous step in PROCESS_JOB_DETAIL TABLE.
      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
            P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => lv_insert_rec,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'UPDATE DONE FOR CUR_PROT_ID_NULL',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            P_ERR_CODE := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      ------------------------------------------------calculate manifest data--------------------------------------------------------------------------
      -----------------------------------------------------------------------------------------------------------------------------------------------
      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
            P_STEP_NAME                 => 'UPDATE',
            P_JOB_AUDIT_MESSAGE         =>    'UPDATE HEADER STG WITH MANIFEST DATA FOR - '
                                           || p_region
                                           || '_'
                                           || p_mod,
            P_NUMBER_OF_ROWS_SELECTED   => NULL,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_IS_BASE_TABLE             => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'Update started..',
            P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
            P_JOB_INSTANCE_ID           => lv_job_instance_id,
            P_JOB_NAME                  => lv_job_name,
            P_JOB_AUDIT_LEVEL_ID        => NULL,
            P_COMMAND_NAME              => 'SQL',
            P_COMMAND_TYPE              => 'UPDATE',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            p_err_code := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      lv_insert_rec := 0;
      lv_selcted_rec := 0;

      IF P_INIT_FLAG = 'Y'
      THEN
         SELECT result_value
           INTO lv_manifest_qry
           FROM scdh_reporting.backlog_rule_setup
          WHERE deriving_parameter = 'MANIFEST_INITIAL_LOAD';
      ELSE
         SELECT result_value
           INTO lv_manifest_qry
           FROM scdh_reporting.backlog_rule_setup
          WHERE deriving_parameter = 'MANIFEST';
      END IF;

      OPEN rc_manifest FOR lv_manifest_qry USING P_REGION, P_MOD;           --

      LOOP
         FETCH rc_manifest BULK COLLECT INTO t_wo_mf_bcklog LIMIT lv_limit2;

         EXIT WHEN t_wo_mf_bcklog.COUNT = 0;

         BEGIN
            FORALL i IN t_wo_mf_bcklog.FIRST .. t_wo_mf_bcklog.LAST
              SAVE EXCEPTIONS
               UPDATE scdh_reporting.OM_BACKLOG_HEADER_STG bh
                  SET ASN_ID = t_wo_mf_bcklog (I).ASN_ID,
                      ASN_STATUS_CODE = t_wo_mf_bcklog (I).ASN_STATUS_CODE,
                      WO_STATUS_CODE = t_wo_mf_bcklog (I).WO_STATUS_CODE,
                      ship_notification_date =
                         t_wo_mf_bcklog (I).SHIP_NOTIFICATION_DATE,
                      channel_status_code =
                         t_wo_mf_bcklog (I).CHANNEL_STATUS_CODE,
                      GOODS_RECEIPT_DATE =
                         t_wo_mf_bcklog (I).GOODS_RECEIPT_DATE,
                      MANIFEST_SHIP_DATE =
                         t_wo_mf_bcklog (I).MANIFEST_SHIP_DATE
                WHERE     REGION_CODE = t_wo_mf_bcklog (I).REGION_CODE
                      AND BU_ID = t_wo_mf_bcklog (I).BU_ID
                      AND ORDER_NUM = t_wo_mf_bcklog (I).ORDER_NUM
                      AND JOB_TIME = 'HOURLY'
                      AND bh.mod_num = P_MOD;

            lv_insert_rec := lv_insert_rec + SQL%ROWCOUNT;
            lv_selcted_rec := lv_selcted_rec + t_wo_mf_bcklog.COUNT;

            COMMIT;
         EXCEPTION
            WHEN OTHERS
            THEN
               FOR i IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
               LOOP
                  IF t_wo_mf_bcklog.EXISTS (
                        SQL%BULK_EXCEPTIONS (i).ERROR_INDEX)
                  THEN
                     lv_indx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
                     lv_loop_cnt := lv_loop_cnt + 1;
                     t_excp_po (lv_loop_cnt).ORDER_NUM :=
                        t_wo_mf_bcklog (lv_indx).ORDER_NUM;
                     t_excp_po (lv_loop_cnt).BU_ID :=
                        t_wo_mf_bcklog (lv_indx).BU_ID;
                     t_excp_po (lv_loop_cnt).REGION_CODE :=
                        t_wo_mf_bcklog (lv_indx).REGION_CODE;
                     t_excp_po (lv_loop_cnt).sys_err_code :=
                        SQL%BULK_EXCEPTIONS (i).ERROR_CODE;
                     t_excp_po (lv_loop_cnt).sys_err_mesg :=
                           SQLERRM (SQL%BULK_EXCEPTIONS (i).ERROR_CODE * -1)
                        || 'ERR LOC '
                        || lv_error_location;
                  END IF;
               END LOOP;
         END;
      END LOOP;

      CLOSE rc_manifest;


      /* Added as part of Merge Backlog requirement*/
      /*Below block will update SHIP_STATUS_CODE column in hte OM_BACKLOG_HEADR_STG table*/
      lv_error_location := 5.1;

      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
            P_STEP_NAME                 => 'UPDATE',
            P_JOB_AUDIT_MESSAGE         =>    'UPDATE DETAIL STG FOR CUR_OSS FOR - '
                                           || p_region
                                           || '_'
                                           || p_mod,
            P_NUMBER_OF_ROWS_SELECTED   => NULL,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_IS_BASE_TABLE             => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'Update started..',
            P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
            P_JOB_INSTANCE_ID           => lv_job_instance_id,
            P_JOB_NAME                  => lv_job_name,
            P_JOB_AUDIT_LEVEL_ID        => NULL,
            P_COMMAND_NAME              => 'SQL',
            P_COMMAND_TYPE              => 'UPDATE',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            p_err_code := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      lv_insert_rec := 0;
      lv_selcted_rec := 0;

      OPEN CUR_OSS (P_REGION, P_MOD);

      LOOP
         FETCH CUR_OSS BULK COLLECT INTO t_oss LIMIT lv_limit3;

         EXIT WHEN t_oss.COUNT = 0;

         BEGIN
            FORALL j IN t_oss.FIRST .. t_oss.LAST SAVE EXCEPTIONS
               UPDATE SCDH_REPORTING.OM_BACKLOG_HEADER_STG
                  SET SHIP_STATUS_CODE = t_oss (j).SHIP_STATUS_CODE
                WHERE ROWID = t_oss (j).HDR_ROW;

            COMMIT;

            lv_insert_rec := lv_insert_rec + SQL%ROWCOUNT;
            lv_selcted_rec := lv_selcted_rec + t_oss.COUNT;
         EXCEPTION
            WHEN OTHERS
            THEN
               FOR i IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
               LOOP
                  IF t_oss.EXISTS (SQL%BULK_EXCEPTIONS (i).ERROR_INDEX)
                  THEN
                     lv_indx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
                     lv_loop_cnt := lv_loop_cnt + 1;
                     t_excp_po (lv_loop_cnt).ORA_ERR_ROWID$ :=
                        t_oss (lv_indx).HDR_ROW;
                     t_excp_po (lv_loop_cnt).ORDER_NUM :=
                        t_oss (lv_indx).ORDER_NUM;

                     t_excp_po (lv_loop_cnt).sys_err_code :=
                        SQL%BULK_EXCEPTIONS (i).ERROR_CODE;
                     t_excp_po (lv_loop_cnt).sys_err_mesg :=
                           SQLERRM (SQL%BULK_EXCEPTIONS (i).ERROR_CODE * -1)
                        || 'ERR LOC '
                        || lv_error_location;
                  END IF;
               END LOOP;
         END;
      END LOOP;

      CLOSE CUR_OSS;

      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
            P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => lv_insert_rec,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'UPDATE DONE FOR CUR_OSS',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            P_ERR_CODE := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      /*Below block will update STATUS_CODE column in hte OM_BACKLOG_HEADR_STG table*/
      lv_error_location := 5.2;

      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
            P_STEP_NAME                 => 'UPDATE',
            P_JOB_AUDIT_MESSAGE         =>    'UPDATE DETAIL STG FOR CUR_SDT FOR - '
                                           || p_region
                                           || '_'
                                           || p_mod,
            P_NUMBER_OF_ROWS_SELECTED   => NULL,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_IS_BASE_TABLE             => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'Update started..',
            P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
            P_JOB_INSTANCE_ID           => lv_job_instance_id,
            P_JOB_NAME                  => lv_job_name,
            P_JOB_AUDIT_LEVEL_ID        => NULL,
            P_COMMAND_NAME              => 'SQL',
            P_COMMAND_TYPE              => 'UPDATE',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            p_err_code := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      lv_insert_rec := 0;
      lv_selcted_rec := 0;

      OPEN CUR_SDT (P_REGION, P_MOD);

      LOOP
         FETCH CUR_SDT BULK COLLECT INTO t_SDT LIMIT lv_limit3;

         EXIT WHEN t_SDT.COUNT = 0;

         BEGIN
            FORALL j IN t_SDT.FIRST .. t_SDT.LAST SAVE EXCEPTIONS
               UPDATE SCDH_REPORTING.OM_BACKLOG_HEADER_STG
                  SET STATUS_CODE = t_SDT (j).STATUS_CODE
                WHERE ROWID = t_SDT (j).HDR_ROW;

            COMMIT;

            lv_insert_rec := lv_insert_rec + SQL%ROWCOUNT;
            lv_selcted_rec := lv_selcted_rec + t_SDT.COUNT;
         EXCEPTION
            WHEN OTHERS
            THEN
               FOR i IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
               LOOP
                  IF t_SDT.EXISTS (SQL%BULK_EXCEPTIONS (i).ERROR_INDEX)
                  THEN
                     lv_indx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
                     lv_loop_cnt := lv_loop_cnt + 1;
                     t_excp_po (lv_loop_cnt).ORA_ERR_ROWID$ :=
                        t_SDT (lv_indx).HDR_ROW;
                     t_excp_po (lv_loop_cnt).ORDER_NUM :=
                        t_SDT (lv_indx).ORDER_NUM;

                     t_excp_po (lv_loop_cnt).sys_err_code :=
                        SQL%BULK_EXCEPTIONS (i).ERROR_CODE;
                     t_excp_po (lv_loop_cnt).sys_err_mesg :=
                           SQLERRM (SQL%BULK_EXCEPTIONS (i).ERROR_CODE * -1)
                        || 'ERR LOC '
                        || lv_error_location;
                  END IF;
               END LOOP;
         END;
      END LOOP;

      CLOSE CUR_SDT;

      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
            P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => lv_insert_rec,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'UPDATE DONE FOR CUR_SDT',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            P_ERR_CODE := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;



      /*Below block will update MSG_TYPE column in hte OM_BACKLOG_HEADR_STG table*/
      lv_error_location := 5.3;

      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
            P_STEP_NAME                 => 'UPDATE',
            P_JOB_AUDIT_MESSAGE         =>    'UPDATE DETAIL STG FOR CUR_AN FOR - '
                                           || p_region
                                           || '_'
                                           || p_mod,
            P_NUMBER_OF_ROWS_SELECTED   => NULL,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_IS_BASE_TABLE             => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'Update started..',
            P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
            P_JOB_INSTANCE_ID           => lv_job_instance_id,
            P_JOB_NAME                  => lv_job_name,
            P_JOB_AUDIT_LEVEL_ID        => NULL,
            P_COMMAND_NAME              => 'SQL',
            P_COMMAND_TYPE              => 'UPDATE',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            p_err_code := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      lv_insert_rec := 0;
      lv_selcted_rec := 0;

      OPEN CUR_AN (P_REGION, P_MOD);

      LOOP
         FETCH CUR_AN BULK COLLECT INTO t_AN LIMIT lv_limit3;

         EXIT WHEN t_AN.COUNT = 0;

         BEGIN
            FORALL j IN t_AN.FIRST .. t_AN.LAST SAVE EXCEPTIONS
               UPDATE SCDH_REPORTING.OM_BACKLOG_HEADER_STG
                  SET MSG_TYPE = t_AN (j).MSG_TYPE
                WHERE ROWID = t_AN (j).HDR_ROW;


            COMMIT;

            lv_insert_rec := lv_insert_rec + SQL%ROWCOUNT;
            lv_selcted_rec := lv_selcted_rec + t_AN.COUNT;
         EXCEPTION
            WHEN OTHERS
            THEN
               FOR i IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
               LOOP
                  IF t_AN.EXISTS (SQL%BULK_EXCEPTIONS (i).ERROR_INDEX)
                  THEN
                     lv_indx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
                     lv_loop_cnt := lv_loop_cnt + 1;
                     t_excp_po (lv_loop_cnt).ORA_ERR_ROWID$ :=
                        t_AN (lv_indx).HDR_ROW;
                     t_excp_po (lv_loop_cnt).ORDER_NUM :=
                        t_AN (lv_indx).ORDER_NUM;

                     t_excp_po (lv_loop_cnt).sys_err_code :=
                        SQL%BULK_EXCEPTIONS (i).ERROR_CODE;
                     t_excp_po (lv_loop_cnt).sys_err_mesg :=
                           SQLERRM (SQL%BULK_EXCEPTIONS (i).ERROR_CODE * -1)
                        || 'ERR LOC '
                        || lv_error_location;
                  END IF;
               END LOOP;
         END;
      END LOOP;

      CLOSE CUR_AN;

      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
            P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => lv_insert_rec,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'UPDATE DONE FOR CUR_AN',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            P_ERR_CODE := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      --OTM_APJ CHANGES STARTS

      --      lv_error_location := 1.431;
      --      IF lv_audit_log_yn = 'Y'
      --      THEN
      --        FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
      --         P_STEP_NAME                 => 'UPDATE',
      --         P_JOB_AUDIT_MESSAGE         => 'UPDATE DETAIL STG FOR CUR_AN_APJ FOR - ' || p_region ||'_'||p_mod,
      --         P_NUMBER_OF_ROWS_SELECTED   => NULL,
      --         P_NUMBER_OF_ROWS_INSERTED   => NULL,
      --         P_NUMBER_OF_ROWS_UPDATED    => NULL,
      --         P_NUMBER_OF_ROWS_DELETED    => NULL,
      --         P_IS_BASE_TABLE             => NULL,
      --         P_RUN_STATUS_CODE           => 0,
      --         P_RUN_STATUS_DESC           => 'Update started..',
      --         P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
      --         P_JOB_INSTANCE_ID           => lv_job_instance_id,
      --         P_JOB_NAME                  => lv_job_name,
      --         P_JOB_AUDIT_LEVEL_ID        => NULL,
      --         P_COMMAND_NAME              => 'SQL',
      --         P_COMMAND_TYPE              => 'UPDATE',
      --         P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
      --         P_ERROR_CODE                => lv_error_code,
      --         P_ERROR_MSG                 => lv_error_msg);
      --
      --         IF lv_error_code <> 0
      --         THEN
      --            p_err_code := lv_error_code;
      --            p_err_mesg :=lv_error_msg || 'Error Location:' || lv_error_location;
      --            RAISE exp_post;
      --         END IF;
      --      END IF;
      --      lv_insert_rec := 0;
      --      lv_selcted_rec := 0;
      --
      --      IF P_REGION = 'APJ' THEN
      --      OPEN CUR_AN_APJ(P_REGION, P_MOD);
      --      LOOP
      --        FETCH CUR_AN_APJ BULK COLLECT INTO t_AN_APJ LIMIT lv_limit3;
      --        EXIT WHEN t_AN_APJ.COUNT = 0;
      --
      --        BEGIN
      --                FORALL j IN t_AN_APJ.FIRST .. t_AN_APJ.LAST SAVE EXCEPTIONS
      --             UPDATE  SCDH_REPORTING.OM_BACKLOG_HEADER_STG
      --                SET MSG_TYPE=t_AN_APJ(j).MSG_TYPE
      --                   WHERE     ROWID=t_AN_apj(j).HDR_ROW  ;
      --
      --
      --            COMMIT;
      --
      --         lv_insert_rec := lv_insert_rec + SQL%ROWCOUNT;
      --         lv_selcted_rec :=lv_selcted_rec + t_AN_APJ.COUNT;
      --          EXCEPTION
      --            WHEN OTHERS
      --            THEN
      --               FOR i IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
      --               LOOP
      --                  IF t_AN_APJ.EXISTS (SQL%BULK_EXCEPTIONS (i).ERROR_INDEX)
      --                  THEN
      --                     lv_indx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
      --                     lv_loop_cnt := lv_loop_cnt + 1;
      --                     t_excp_po (lv_loop_cnt).ORA_ERR_ROWID$:=t_AN_APJ(lv_indx).HDR_ROW;
      --                     t_excp_po (lv_loop_cnt).ORDER_NUM:=t_AN_APJ(lv_indx).ORDER_NUM;
      --
      --                     t_excp_po (lv_loop_cnt).sys_err_code  :=SQL%BULK_EXCEPTIONS (i).ERROR_CODE;
      --                     t_excp_po (lv_loop_cnt).sys_err_mesg  :=SQLERRM (SQL%BULK_EXCEPTIONS (i).ERROR_CODE * -1)||'ERR LOC '||lv_error_location;
      --                  END IF;
      --               END LOOP;
      --         END;
      --      END LOOP;
      --      CLOSE CUR_AN_APJ;
      --      IF lv_audit_log_yn = 'Y'
      --          THEN
      --          FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
      --             P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
      --             P_NUMBER_OF_ROWS_INSERTED   => NULL,
      --             P_NUMBER_OF_ROWS_UPDATED    => lv_insert_rec,
      --             P_NUMBER_OF_ROWS_DELETED    => NULL,
      --             P_RUN_STATUS_CODE           => 0,
      --             P_RUN_STATUS_DESC           => 'UPDATE DONE FOR CUR_AN_APJ',
      --             P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
      --             P_ERROR_CODE                => lv_error_code,
      --             P_ERROR_MSG                 => lv_error_msg);
      --
      --          IF lv_error_code <> 0
      --          THEN
      --             P_ERR_CODE := lv_error_code;
      --             p_err_mesg := lv_error_msg || 'Error Location:' || lv_error_location;
      --             RAISE exp_post;
      --          END IF;
      --      END IF;
      --      END IF;
      --OTM_APJ CHANGES ENDS

      /*Below block will update SO_ATTRIBUTE column in hte OM_BACKLOG_HEADR_STG table*/
      lv_error_location := 5.4;

      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
            P_STEP_NAME                 => 'UPDATE',
            P_JOB_AUDIT_MESSAGE         =>    'UPDATE HEADER STG FOR CUR_SOATB FOR - '
                                           || p_region
                                           || '_'
                                           || p_mod,
            P_NUMBER_OF_ROWS_SELECTED   => NULL,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_IS_BASE_TABLE             => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'Update started..',
            P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
            P_JOB_INSTANCE_ID           => lv_job_instance_id,
            P_JOB_NAME                  => lv_job_name,
            P_JOB_AUDIT_LEVEL_ID        => NULL,
            P_COMMAND_NAME              => 'SQL',
            P_COMMAND_TYPE              => 'UPDATE',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            p_err_code := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      lv_insert_rec := 0;
      lv_selcted_rec := 0;

      IF P_REGION = 'EMEA' OR P_REGION = 'APJ'
      THEN
         OPEN CUR_SOATB (P_REGION, P_MOD);

         LOOP
            FETCH CUR_SOATB BULK COLLECT INTO t_SOATB LIMIT lv_limit3;

            EXIT WHEN t_SOATB.COUNT = 0;

            BEGIN
               FORALL j IN t_SOATB.FIRST .. t_SOATB.LAST SAVE EXCEPTIONS
                  UPDATE SCDH_REPORTING.OM_BACKLOG_HEADER_STG
                     SET SO_ATTRIBUTE = t_SOATB (j).ATTRIBUTE_NAME
                   WHERE ROWID = t_SOATB (j).HDR_ROW;

               COMMIT;

               lv_insert_rec := lv_insert_rec + SQL%ROWCOUNT;
               lv_selcted_rec := lv_selcted_rec + t_SOATB.COUNT;
            EXCEPTION
               WHEN OTHERS
               THEN
                  FOR i IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
                  LOOP
                     IF t_SOATB.EXISTS (SQL%BULK_EXCEPTIONS (i).ERROR_INDEX)
                     THEN
                        lv_indx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
                        lv_loop_cnt := lv_loop_cnt + 1;
                        t_excp_po (lv_loop_cnt).ORA_ERR_ROWID$ :=
                           t_SOATB (lv_indx).HDR_ROW;
                        t_excp_po (lv_loop_cnt).ORDER_NUM :=
                           t_SOATB (lv_indx).ORDER_NUM;

                        t_excp_po (lv_loop_cnt).sys_err_code :=
                           SQL%BULK_EXCEPTIONS (i).ERROR_CODE;
                        t_excp_po (lv_loop_cnt).sys_err_mesg :=
                              SQLERRM (
                                 SQL%BULK_EXCEPTIONS (i).ERROR_CODE * -1)
                           || 'ERR LOC '
                           || lv_error_location;
                     END IF;
                  END LOOP;
            END;
         END LOOP;

         CLOSE CUR_SOATB;
      END IF;

      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
            P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => lv_insert_rec,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'UPDATE DONE FOR CUR_SOATB',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            P_ERR_CODE := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      ---updates the status of previous step in PROCESS_JOB_DETAIL TABLE.
      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
            P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => lv_insert_rec,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'UPDATE DONE FOR rc_manifest',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            P_ERR_CODE := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      ------------------------------------------------CALCULATE STAGE AND SUB-STAGE---------------------------------------------------------------------
      dbms_output.put_line ('Before prc_calculate_stg_sub_stg' );
      SCDH_CODE.om_piece_part_bcklg_pkg.prc_calculate_stg_sub_stg (
         p_region,
         p_mod,
         p_err_code,
         p_err_mesg);                                                       --

      IF p_err_code <> 0
      THEN
         RAISE exp_post;
      END IF;

       --Begin <RK001> --to mark Consumed for Non Tied Orders
      lv_error_location := 5.5;
      dbms_output.put_line('for non tied unconsumed');
     IF p_region = 'AMER' THEN
 --ReadyStock Pick updation Begins <RK006>
   IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
               P_STEP_NAME                 => 'UPDATE/DELETE',
               P_JOB_AUDIT_MESSAGE         =>    'UPDATE/DELETE BEGIN FOR READYSTOCK PICK ORDERS IN DETAIL - '
                                               || p_region
                                           || '_'
                                           || p_mod,
               P_NUMBER_OF_ROWS_SELECTED   => NULL,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => NULL,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_IS_BASE_TABLE             => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'Updation started...',
               P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
               P_JOB_INSTANCE_ID           => lv_job_instance_id,
               P_JOB_NAME                  => lv_job_name,
               P_JOB_AUDIT_LEVEL_ID        => NULL,
               P_COMMAND_NAME              => 'SQL',
               P_COMMAND_TYPE              => 'UPDATE/DELETE',
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);

            IF lv_error_code <> 0
            THEN
               p_err_code := lv_error_code;
               p_err_mesg :=
                  lv_error_msg || 'Error Location:' || lv_error_location;
               RAISE exp_post;
            END IF;
         END IF;

         lv_insert_rec := 0;
        lv_selcted_rec := 0;

        OPEN Cur_ReadyStock_Pick(P_REGION, P_MOD);
        LOOP
          FETCH Cur_ReadyStock_Pick BULK COLLECT INTO t_Stock_Pick LIMIT lv_limit3;

          EXIT WHEN t_Stock_Pick.COUNT = 0;

          FOR j IN t_Stock_Pick.FIRST .. t_Stock_Pick.LAST
          LOOP

           BEGIN
              IF     t_stock_pick (j).rsid IS NOT NULL
                 AND t_stock_pick (j).system_type_code = 'SYS'
                 AND t_stock_pick (j).backlog_order_tie_type in ('CTO-CUST-PICK-FINISHEDGOODS','BTO-CUST-PICK-FINISHEDGOODS')
             THEN
                DELETE FROM scdh_reporting.om_backlog_detail_stg
                WHERE  region_code = p_region
                AND job_time = 'HOURLY'
                AND order_num = t_stock_pick (j).order_num
                AND prod_order_num = t_stock_pick (j).prod_order_num
                AND tie_number = t_stock_pick (j).tie_number
                AND bu_id = t_stock_pick (j).bu_id
                AND mod_num = t_stock_pick (j).mod_num;
            ELSIF  t_stock_pick (j).rsid IS NOT NULL
                AND t_stock_pick (j).system_type_code = 'NON-SYS'
                AND t_stock_pick (j).backlog_order_tie_type = 'CUST-PICK-OPTIONS'
           THEN
                UPDATE scdh_reporting.om_backlog_detail_stg
                SET consumption_status = 'CONSUMED',
                    Backlog_Facility = Consumption_facility
               WHERE  region_code = p_region
               AND job_time = 'HOURLY'
               AND order_num = t_stock_pick (j).order_num
               AND prod_order_num = t_stock_pick (j).prod_order_num
               AND tie_number = t_stock_pick (j).tie_number
               AND bu_id = t_stock_pick (j).bu_id
               AND mod_num = t_stock_pick (j).mod_num;

           END IF;
               lv_insert_rec := lv_insert_rec + SQL%ROWCOUNT;
               lv_selcted_rec := lv_selcted_rec + t_stock_pick.COUNT;
         EXCEPTION
           WHEN OTHERS
           THEN
                lv_loop_cnt := lv_loop_cnt + 1;
                t_excp_po (lv_loop_cnt).order_num := t_stock_pick (j).order_num;
                t_excp_po (lv_loop_cnt).region_code := p_region;
                t_excp_po (lv_loop_cnt).sys_err_code := SQLCODE;
                t_excp_po (lv_loop_cnt).sys_err_mesg := SQLERRM || 'Error Location:' || lv_error_location;
          END;

       END LOOP;

     END LOOP;
   CLOSE Cur_ReadyStock_Pick;


        ------------------------------------------
     lv_error_location := 5.6;
     IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
            P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => lv_insert_rec,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'UPDATE/DELETE DONE FOR READYSTOCK PICK ORDERS IN DETAIL',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            P_ERR_CODE := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;


----ReadyStock Pick updation  End <RK006>------------------
----E2E Begins-------------------
       IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
               P_STEP_NAME                 => 'UPDATE',
               P_JOB_AUDIT_MESSAGE         =>    'UPDATE BEGIN FOR NONTIED UNCONSUMED ORDERS_3PL_ASN IN HEADER - '
                                               || p_region
                                           || '_'
                                           || p_mod,
               P_NUMBER_OF_ROWS_SELECTED   => NULL,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => NULL,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_IS_BASE_TABLE             => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'Updation started...',
               P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
               P_JOB_INSTANCE_ID           => lv_job_instance_id,
               P_JOB_NAME                  => lv_job_name,
               P_JOB_AUDIT_LEVEL_ID        => NULL,
               P_COMMAND_NAME              => 'SQL',
               P_COMMAND_TYPE              => 'UPDATE',
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);

            IF lv_error_code <> 0
            THEN
               p_err_code := lv_error_code;
               p_err_mesg :=
                  lv_error_msg || 'Error Location:' || lv_error_location;
               RAISE exp_post;
            END IF;
         END IF;

        lv_insert_rec := 0;
        lv_selcted_rec := 0;

        open cur_hdr_cons_status_nontied(P_REGION, P_MOD);
        Loop
          FETCH cur_hdr_cons_status_nontied BULK COLLECT INTO t_hdr_upd_cons_nontied LIMIT lv_limit3;

            EXIT WHEN t_hdr_upd_cons_nontied.COUNT = 0;
           BEGIN
                  FORALL j in  t_hdr_upd_cons_nontied.FIRST .. t_hdr_upd_cons_nontied.LAST SAVE EXCEPTIONS
                  UPDATE SCDH_REPORTING.OM_BACKLOG_HEADER_STG
                     SET ASN_3PL = t_hdr_upd_cons_nontied(J).SOURCE_MANIFEST_ID,
                         ASN_3PL_STATUS = t_hdr_upd_cons_nontied(J).MANIFEST_STATUS_CODE,
                         SYS_LAST_MODIFIED_DATE =  SYSTIMESTAMP
                   WHERE ROWID = t_hdr_upd_cons_nontied (j).HDR_ROW
                     AND REGION_CODE = P_REGION
                     AND MOD_NUM = P_MOD
                     AND JOB_TIME = 'HOURLY';

               lv_insert_rec := lv_insert_rec + SQL%ROWCOUNT;
               lv_selcted_rec := lv_selcted_rec + t_hdr_upd_cons_nontied.COUNT;
              COMMIT;
            EXCEPTION
               WHEN OTHERS
               THEN
                dbms_output.put_line('in_non_tied cursor:'||sqlerrm);
                  FOR i IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
                  LOOP
                    IF t_hdr_upd_cons_nontied.EXISTS (SQL%BULK_EXCEPTIONS (i).ERROR_INDEX)
                     THEN
                        lv_indx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
                        lv_loop_cnt := lv_loop_cnt + 1;
                        t_excp_po (lv_loop_cnt).ORA_ERR_ROWID$ :=
                           t_hdr_upd_cons_nontied (lv_indx).HDR_ROW;
                        t_excp_po (lv_loop_cnt).ORDER_NUM :=
                           t_hdr_upd_cons_nontied (lv_indx).ORDER_NUM;
                        t_excp_po (lv_loop_cnt).sys_err_code :=
                           SQL%BULK_EXCEPTIONS (i).ERROR_CODE;
                        t_excp_po (lv_loop_cnt).sys_err_mesg :=
                              SQLERRM (
                                 SQL%BULK_EXCEPTIONS (i).ERROR_CODE * -1)
                           || 'ERR LOC '
                           || lv_error_location;
                     END IF;
                  END LOOP;
               END;
              END LOOP;
         CLOSE cur_hdr_cons_status_nontied;

      lv_error_location := 5.7;
      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
            P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => lv_insert_rec,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'UPDATE DONE FOR NONTIED UNCONSUMED ORDERS_3PL_ASN IN HEADER',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            P_ERR_CODE := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;
       --------------------------------------------------------------------
       lv_error_location := 5.8;
       IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
               P_STEP_NAME                 => 'UPDATE',
               P_JOB_AUDIT_MESSAGE         =>    'UPDATE BEGIN FOR NONTIED UNCONSUMED ORDERS IN DETAIL - '
                                               || p_region
                                           || '_'
                                           || p_mod,
               P_NUMBER_OF_ROWS_SELECTED   => NULL,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => NULL,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_IS_BASE_TABLE             => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'Updation started...',
               P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
               P_JOB_INSTANCE_ID           => lv_job_instance_id,
               P_JOB_NAME                  => lv_job_name,
               P_JOB_AUDIT_LEVEL_ID        => NULL,
               P_COMMAND_NAME              => 'SQL',
               P_COMMAND_TYPE              => 'UPDATE',
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);

            IF lv_error_code <> 0
            THEN
               p_err_code := lv_error_code;
               p_err_mesg :=
                  lv_error_msg || 'Error Location:' || lv_error_location;
               RAISE exp_post;
            END IF;
         END IF;

        lv_insert_rec := 0;
        lv_selcted_rec := 0;
        open cur_cons_status_Nontied(P_REGION, P_MOD);
        Loop
          FETCH cur_cons_status_Nontied BULK COLLECT INTO t_col_upd_cons_non LIMIT lv_limit3;

            EXIT WHEN t_col_upd_cons_non.COUNT = 0;

            BEGIN
                 FORALL j IN t_col_upd_cons_non.FIRST .. t_col_upd_cons_non.LAST SAVE EXCEPTIONS

                  UPDATE SCDH_REPORTING.OM_BACKLOG_DETAIL_STG
                     SET CONSUMPTION_STATUS= t_col_upd_cons_non(J).STATUS,
                         BACKLOG_FACILITY =   t_col_upd_cons_non(j).consumption_facility,
                         SYS_LAST_MODIFIED_DATE =  SYSTIMESTAMP
                   WHERE ROWID = t_col_upd_cons_non (j).HDR_ROW
                     AND REGION_CODE = P_REGION
                     AND MOD_NUM = P_MOD
                     AND JOB_TIME = 'HOURLY';

               lv_insert_rec := lv_insert_rec + SQL%ROWCOUNT;
               lv_selcted_rec := lv_selcted_rec + t_col_upd_cons_non.COUNT;

            EXCEPTION
               WHEN OTHERS
               THEN
                  FOR i IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
                  LOOP
                    IF t_col_upd_cons_non.EXISTS (SQL%BULK_EXCEPTIONS (i).ERROR_INDEX)
                     THEN
                        lv_indx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
                        lv_loop_cnt := lv_loop_cnt + 1;
                        t_excp_po (lv_loop_cnt).ORA_ERR_ROWID$ :=
                           t_col_upd_cons_non (lv_indx).HDR_ROW;
                        t_excp_po (lv_loop_cnt).ORDER_NUM :=
                           t_col_upd_cons_non (lv_indx).ORDER_NUM;
                        t_excp_po (lv_loop_cnt).sys_err_code :=
                           SQL%BULK_EXCEPTIONS (i).ERROR_CODE;
                        t_excp_po (lv_loop_cnt).sys_err_mesg :=
                              SQLERRM (
                                 SQL%BULK_EXCEPTIONS (i).ERROR_CODE * -1)
                           || 'ERR LOC '
                           || lv_error_location;
                     END IF;
                  END LOOP;
            END;
            END LOOP;
         CLOSE cur_cons_status_Nontied;

     lv_error_location := 5.9;
     IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
            P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => lv_insert_rec,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'UPDATE DONE FOR NONTIED UNCONSUMED ORDERS IN DETAIL',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            P_ERR_CODE := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      dbms_output.put_line('update done for non tied unconsumed');
    --End <RK001>
    --Begin <RK002>-- To mark Consumed for System Tied orders
       lv_error_location := 6.0;
       IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
               P_STEP_NAME                 => 'UPDATE',
               P_JOB_AUDIT_MESSAGE         =>    'UPDATE BEGIN FOR SYSTEM UNCONSUMED ORDERS_3PL_ASN IN HEADER - '
                                               || p_region
                                           || '_'
                                           || p_mod,
               P_NUMBER_OF_ROWS_SELECTED   => NULL,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => NULL,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_IS_BASE_TABLE             => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'Updation started...',
               P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
               P_JOB_INSTANCE_ID           => lv_job_instance_id,
               P_JOB_NAME                  => lv_job_name,
               P_JOB_AUDIT_LEVEL_ID        => NULL,
               P_COMMAND_NAME              => 'SQL',
               P_COMMAND_TYPE              => 'UPDATE',
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);

            IF lv_error_code <> 0
            THEN
               p_err_code := lv_error_code;
               p_err_mesg :=
                  lv_error_msg || 'Error Location:' || lv_error_location;
               RAISE exp_post;
            END IF;
         END IF;


       lv_insert_rec := 0;
       lv_selcted_rec := 0;

       open cur_hdr_cons_status_tied(P_REGION, P_MOD);
        Loop
          FETCH cur_hdr_cons_status_tied BULK COLLECT INTO t_hdr_upd_cons_tied LIMIT lv_limit3;

            EXIT WHEN t_hdr_upd_cons_tied.COUNT = 0;

              BEGIN
                  FORALL j IN t_hdr_upd_cons_tied.FIRST .. t_hdr_upd_cons_tied.LAST SAVE EXCEPTIONS
                  UPDATE SCDH_REPORTING.OM_BACKLOG_HEADER_STG
                     SET ASN_3PL = t_hdr_upd_cons_tied(J).SOURCE_MANIFEST_ID,
                         ASN_3PL_STATUS = t_hdr_upd_cons_tied(J).MANIFEST_STATUS_CODE,
                         SYS_LAST_MODIFIED_DATE =  SYSTIMESTAMP
                   WHERE ROWID = t_hdr_upd_cons_tied (j).HDR_ROW
                     AND REGION_CODE = P_REGION
                     AND MOD_NUM = P_MOD
                     AND JOB_TIME = 'HOURLY';

               lv_insert_rec := lv_insert_rec + SQL%ROWCOUNT;
               lv_selcted_rec := lv_selcted_rec + t_hdr_upd_cons_tied.COUNT;
               COMMIT;

               EXCEPTION
               WHEN OTHERS
               THEN
                dbms_output.put_line('in_tied cursor:'||sqlerrm);
                  FOR i IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
                  LOOP
                    IF t_hdr_upd_cons_tied.EXISTS (SQL%BULK_EXCEPTIONS (i).ERROR_INDEX)
                     THEN
                        lv_indx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
                        lv_loop_cnt := lv_loop_cnt + 1;
                        t_excp_po (lv_loop_cnt).ORA_ERR_ROWID$ :=
                           t_hdr_upd_cons_tied (lv_indx).HDR_ROW;
                        t_excp_po (lv_loop_cnt).ORDER_NUM :=
                           t_hdr_upd_cons_tied (lv_indx).ORDER_NUM;
                        t_excp_po (lv_loop_cnt).sys_err_code :=
                           SQL%BULK_EXCEPTIONS (i).ERROR_CODE;
                        t_excp_po (lv_loop_cnt).sys_err_mesg :=
                              SQLERRM (
                                 SQL%BULK_EXCEPTIONS (i).ERROR_CODE * -1)
                           || 'ERR LOC '
                           || lv_error_location;
                     END IF;
                  END LOOP;
              END;
          END LOOP;
        CLOSE cur_hdr_cons_status_tied;

      lv_error_location := 6.1;
      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
            P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => lv_insert_rec,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'UPDATE DONE FOR SYSTEM UNCONSUMED ORDERS_3PL_ASN IN HEADER',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            P_ERR_CODE := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

       --------------------------------------------------------------------------
       lv_error_location := 6.2;
       IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
               P_STEP_NAME                 => 'UPDATE',
               P_JOB_AUDIT_MESSAGE         =>    'UPDATE BEGIN FOR SYSTEM UNCONSUMED ORDERS IN DETAIL - '
                                               || p_region
                                           || '_'
                                           || p_mod,
               P_NUMBER_OF_ROWS_SELECTED   => NULL,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => NULL,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_IS_BASE_TABLE             => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'Updation started...',
               P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
               P_JOB_INSTANCE_ID           => lv_job_instance_id,
               P_JOB_NAME                  => lv_job_name,
               P_JOB_AUDIT_LEVEL_ID        => NULL,
               P_COMMAND_NAME              => 'SQL',
               P_COMMAND_TYPE              => 'UPDATE',
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => lv_error_code,
               P_ERROR_MSG                 => lv_error_msg);

            IF lv_error_code <> 0
            THEN
               p_err_code := lv_error_code;
               p_err_mesg :=
                  lv_error_msg || 'Error Location:' || lv_error_location;
               RAISE exp_post;
            END IF;
         END IF;


       lv_insert_rec := 0;
       lv_selcted_rec := 0;

       open cur_cons_status_tied(P_REGION, P_MOD);
        Loop
          FETCH cur_cons_status_tied BULK COLLECT INTO t_col_upd_cons_tied LIMIT lv_limit3;

            EXIT WHEN t_col_upd_cons_tied.COUNT = 0;

            BEGIN
               FORALL j IN t_col_upd_cons_tied.FIRST .. t_col_upd_cons_tied.LAST SAVE EXCEPTIONS

                 UPDATE SCDH_REPORTING.OM_BACKLOG_DETAIL_STG
                     SET CONSUMPTION_STATUS = t_col_upd_cons_tied (j).status,
                         BACKLOG_FACILITY = t_col_upd_cons_tied (j).consumption_facility,
                         SYS_LAST_MODIFIED_DATE =  SYSTIMESTAMP
                   WHERE ROWID = t_col_upd_cons_tied (j).HDR_ROW
                     AND REGION_CODE = P_REGION
                     AND MOD_NUM = P_MOD
                     AND JOB_TIME = 'HOURLY';

              lv_insert_rec := lv_insert_rec + SQL%ROWCOUNT;
              lv_selcted_rec := lv_selcted_rec + t_col_upd_cons_tied.COUNT;
            EXCEPTION
               WHEN OTHERS
               THEN
                dbms_output.put_line('in_non_tied cursor:'||sqlerrm);
                  FOR i IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
                  LOOP
                    IF t_col_upd_cons_tied.EXISTS (SQL%BULK_EXCEPTIONS (i).ERROR_INDEX)
                     THEN
                        lv_indx := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX;
                        lv_loop_cnt := lv_loop_cnt + 1;
                        t_excp_po (lv_loop_cnt).ORA_ERR_ROWID$ :=
                           t_col_upd_cons_tied (lv_indx).HDR_ROW;
                        t_excp_po (lv_loop_cnt).ORDER_NUM :=
                           t_col_upd_cons_tied (lv_indx).ORDER_NUM;
                        t_excp_po (lv_loop_cnt).sys_err_code :=
                           SQL%BULK_EXCEPTIONS (i).ERROR_CODE;
                        t_excp_po (lv_loop_cnt).sys_err_mesg :=
                              SQLERRM (
                                 SQL%BULK_EXCEPTIONS (i).ERROR_CODE * -1)
                           || 'ERR LOC '
                           || lv_error_location;
                     END IF;
                  END LOOP;
               END;

        END LOOP;
        CLOSE cur_cons_status_tied;

      lv_error_location := 6.3;
      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
            P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => lv_insert_rec,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'UPDATE DONE FOR SYSTEM UNCONSUMED ORDERS DETAIL',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            P_ERR_CODE := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

        dbms_output.put_line('update done for tied unconsumed');
     END IF;
   --End <RK002>
      --------------------------------------------------------------------------------------------------------------------------------------------------
      ----------------------------------MERGE INTO scdh_reporting.om_backlog_header -----------------------------------
      lv_error_location := 6.4;

      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
            P_STEP_NAME                 => 'MERGE',
            P_JOB_AUDIT_MESSAGE         =>    'MERGE HEADER AND DETAIL FROM STG FOR - '
                                           || p_region
                                           || '_'
                                           || p_mod,
            P_NUMBER_OF_ROWS_SELECTED   => NULL,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_IS_BASE_TABLE             => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'MERGE started..',
            P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
            P_JOB_INSTANCE_ID           => lv_job_instance_id,
            P_JOB_NAME                  => lv_job_name,
            P_JOB_AUDIT_LEVEL_ID        => NULL,
            P_COMMAND_NAME              => 'SQL',
            P_COMMAND_TYPE              => 'MERGE',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            p_err_code := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      lv_insert_rec := 0;
      lv_selcted_rec := 0;


      --  Truncate Target Table Partition for initial data load
      /*
      IF P_INIT_FLAG = 'Y' THEN
          SCDH_REPORTING.PPB2_UTILITY_pkg.TRUNCATE_TABLE_PARTITION ('OM_BACKLOG_HEADER',P_REGION,VAR_err_code,VAR_ERR_MSG);
          SCDH_REPORTING.PPB2_UTILITY_pkg.TRUNCATE_TABLE_PARTITION ('OM_BACKLOG_DETAIL',P_REGION,VAR_err_code,VAR_ERR_MSG);
      END IF ;
      */

      OPEN cur_hdr_stg_to_main (P_REGION, p_mod);                           --

      LOOP
         FETCH cur_hdr_stg_to_main
            BULK COLLECT INTO t_hdr_stg_to_main
            LIMIT lv_limit1;

         EXIT WHEN t_hdr_stg_to_main.COUNT = 0;
          dbms_output.put_line('count in stg records:'||t_hdr_stg_to_main.count);
         lv_error_location := 6.5;

         /*----------------deleting all the orders from main table which got changed in source after last run ----------------------*/

         FORALL f IN 1 .. t_hdr_stg_to_main.COUNT
            UPDATE SCDH_REPORTING.OM_BACKLOG_HEADER
               SET SYS_ENT_STATE = 'DELETED',
                   job_time = 'DELETED',
                   sys_last_modified_date = SYSTIMESTAMP
             WHERE     ORDER_NUM = t_hdr_stg_to_main (f).ORDER_NUM
                   AND PROD_ORDER_NUM = t_hdr_stg_to_main (f).PROD_ORDER_NUM
                   AND BU_ID = t_hdr_stg_to_main (f).BU_ID
                   AND TIE_NUMBER = t_hdr_stg_to_main (f).TIE_NUMBER
                   AND REGION_CODE = p_region
                   AND JOB_TIME = 'HOURLY';

         lv_error_location := 6.6;

         /* -------------------------------------Inserting the orders with new values-------------------------------------------------*/
         BEGIN
            dbms_output.put_line('inserting into main header table:');
            FORALL w IN 1 .. t_hdr_stg_to_main.COUNT SAVE EXCEPTIONS
               INSERT
                 INTO scdh_reporting.OM_BACKLOG_HEADER (
                         PROD_ORDER_NUM,
                         PROD_ORDER_LINE,
                         ORDER_NUM,
                         BU_ID,
                         REGION_CODE,
                         TIE_NUMBER,
                         PO_NUM,
                         JOB_TIME,
                         QUOTE_NUMBER,
                         OMS_DOMS_STATUS,
                         DOMS_STATUS,
                         CYCLE_STAGE_CODE,
                         CYCLE_SUBSTAGE_CODE,
                         CYCLE_DATE,
                         SSC_NAME,
                         SS_TYPE_NAME,
                         MERGE_TYPE,
                         ORDER_TYPE,
                         BACKLOG_TYPE,
                         SUB_RGN_CDE,
                         SOURCE_LOCAL_CHANNEL_CODE,
                         ORDER_DATE,
                         CANCEL_DATE,
                         IS_RETAIL,
                         SHIP_BY_DATE,
                         MUST_ARRIVE_BY_DATE,
                         CFI_FLAG,
                         TP_FACILITY,
                         SHIP_TO_FACILITY,
                         PLANNED_MERGE_FACILITY,
                         FULFILLMENT_REGION_CODE,
                         CUSTOMER_PICK_UP_FLAG,
                         SHIP_METHOD_CODE,
                         SHIP_MODE_CODE,
                         SHIP_CODE,
                         DIRECT_SHIP_FLAG,
                         SYSTEM_TYPE_CODE,
                         BASE_PROD_PROT_ID,
                         BASE_PROD_CODE,
                         FMLY_PARNT_NAME,
                         WORK_CENTER,
                         ASN_ID,
                         ASN_STATUS_CODE,
                         HOLD_CODE,
                         HOLD_FLAG,
                         CUSTOMER_NUM,
                         CUSTOMER_NAME,
                         AI_DOMS_ORDER_ID,
                         RUSH_FLAG,
                         BUILD_TYPE,
                         ASN_LIFE_CYCLE_SUB_STATUS,
                         WO_STATUS_CODE,
                         CHANNEL_STATUS_CODE,
                         OFS_STATUS,
                         BACKLOG_ORDER_TIE_TYPE,
                         ORDER_SOURCE,
                         Fulf_Order_Type,
                         BASE_SKU,
                         CCN,
                         SYS_ENT_STATE,
                         RUN_DATE,
                         SYS_SOURCE,
                         SYS_CREATION_DATE,
                         SYS_LAST_MODIFIED_DATE,
                         BASE_TYPE,
                         FULF_CHANNEL,
                         IBU_ID,
                         IP_DATE,
                         ITEM_QTY,
                         ORDER_PRIORITY,
                         PRIMARY_FC,
                         QTY_REQD,
                         RETAILER_NAME,
                         RPT_DOMS_STATUS,
                         SHIP_TO_COUNTRY,
                         SHIP_TO_STATE,
                         SUB_CHANNEL,
                         SYSTEM_QTY,
                         WORKCENTER,
                         SO_ATTRIBUTE,
                         -- Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3) begin
                         LOCAL_ORDER_TYPE,
                         MABD_FLAG,
                         FDD_DATE,
                         FDD_FLAG,
                         DP_ENABLED_FLAG,
                         ORIGINAL_RELEASE_DATE,
                         REVISED_RELEASE_DATE,
                         ESD,
                         EST_DELIVERY_DATE,
                         V_P_FLAG, -- Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3) end
                         SALESREP_NAME,
                         asn_3pl,
                         asn_3pl_status,
                         om_order_type,  --<RK004>
                         rsid)      --<RK004>
               VALUES (t_hdr_stg_to_main (w).PROD_ORDER_NUM,
                       t_hdr_stg_to_main (w).PROD_ORDER_LINE,
                       t_hdr_stg_to_main (w).ORDER_NUM,
                       t_hdr_stg_to_main (w).BU_ID,
                       t_hdr_stg_to_main (w).REGION_CODE,
                       t_hdr_stg_to_main (w).TIE_NUMBER,
                       t_hdr_stg_to_main (w).PO_NUM,
                       t_hdr_stg_to_main (w).JOB_TIME,
                       t_hdr_stg_to_main (w).QUOTE_NUMBER,
                       t_hdr_stg_to_main (w).OMS_DOMS_STATUS,
                       t_hdr_stg_to_main (w).DOMS_STATUS,
                       t_hdr_stg_to_main (w).CYCLE_STAGE_CODE,
                       t_hdr_stg_to_main (w).CYCLE_SUBSTAGE_CODE,
                       t_hdr_stg_to_main (w).CYCLE_DATE,
                       t_hdr_stg_to_main (w).SSC_NAME,
                       t_hdr_stg_to_main (w).SS_TYPE_NAME,
                       t_hdr_stg_to_main (w).MERGE_TYPE,
                       t_hdr_stg_to_main (w).ORDER_TYPE,
                       t_hdr_stg_to_main (w).BACKLOG_TYPE,
                       t_hdr_stg_to_main (w).SUB_RGN_CDE,
                       --t_hdr_stg_to_main(w).DEMAND_SUPPLY_REGION_CODE,
                       t_hdr_stg_to_main (w).SOURCE_LOCAL_CHANNEL_CODE,
                       t_hdr_stg_to_main (w).ORDER_DATE,
                       t_hdr_stg_to_main (w).CANCEL_DATE,
                       t_hdr_stg_to_main (w).IS_RETAIL,
                       t_hdr_stg_to_main (w).SHIP_BY_DATE,
                       t_hdr_stg_to_main (w).MUST_ARRIVE_BY_DATE,
                       t_hdr_stg_to_main (w).CFI_FLAG,
                       t_hdr_stg_to_main (w).TP_FACILITY,
                       t_hdr_stg_to_main (w).SHIP_TO_FACILITY,
                       t_hdr_stg_to_main (w).PLANNED_MERGE_FACILITY,
                       t_hdr_stg_to_main (w).FULFILLMENT_REGION_CODE,
                       t_hdr_stg_to_main (w).CUSTOMER_PICK_UP_FLAG,
                       t_hdr_stg_to_main (w).SHIP_METHOD_CODE,
                       t_hdr_stg_to_main (w).SHIP_MODE_CODE,
                       t_hdr_stg_to_main (w).SHIP_CODE,
                       --t_hdr_stg_to_main(w).PUR_CCN,
                       t_hdr_stg_to_main (w).DIRECT_SHIP_FLAG,
                       t_hdr_stg_to_main (w).SYSTEM_TYPE_CODE,
                       t_hdr_stg_to_main (w).BASE_PROD_PROT_ID,
                       t_hdr_stg_to_main (w).BASE_PROD_CODE,
                       t_hdr_stg_to_main (w).FMLY_PARNT_NAME,
                       t_hdr_stg_to_main (w).WORK_CENTER,
                       t_hdr_stg_to_main (w).ASN_ID,
                       t_hdr_stg_to_main (w).ASN_STATUS_CODE,
                       t_hdr_stg_to_main (w).HOLD_CODE,
                       t_hdr_stg_to_main (w).HOLD_FLAG,
                       t_hdr_stg_to_main (w).CUSTOMER_NUM,
                       t_hdr_stg_to_main (w).CUSTOMER_NAME,
                       t_hdr_stg_to_main (w).AI_DOMS_ORDER_ID,
                       t_hdr_stg_to_main (w).RUSH_FLAG,
                       --t_hdr_stg_to_main(w).BUILD_CCN,
                       t_hdr_stg_to_main (w).BUILD_TYPE,
                       t_hdr_stg_to_main (w).ASN_LIFE_CYCLE_SUB_STATUS,
                       t_hdr_stg_to_main (w).WO_STATUS_CODE,
                       t_hdr_stg_to_main (w).CHANNEL_STATUS_CODE,
                       t_hdr_stg_to_main (w).OFS_STATUS,
                       t_hdr_stg_to_main (w).BACKLOG_ORDER_TIE_TYPE,
                       t_hdr_stg_to_main (w).ORDER_SOURCE,
                       t_hdr_stg_to_main (w).Fulf_Order_Type,
                       t_hdr_stg_to_main (w).BASE_SKU,
                       t_hdr_stg_to_main (w).CCN,
                       --t_hdr_stg_to_main(w).SHIP_NOTIFICATION_DATE,
                       t_hdr_stg_to_main (w).SYS_ENT_STATE,
                       t_hdr_stg_to_main (w).RUN_DATE,
                       t_hdr_stg_to_main (w).SYS_SOURCE,
                       t_hdr_stg_to_main (w).SYS_CREATION_DATE,
                       t_hdr_stg_to_main (w).SYS_LAST_MODIFIED_DATE,
                       t_hdr_stg_to_main (w).BASE_TYPE,
                       t_hdr_stg_to_main (w).FULF_CHANNEL,
                       t_hdr_stg_to_main (w).IBU_ID,
                       t_hdr_stg_to_main (w).IP_DATE,
                       t_hdr_stg_to_main (w).ITEM_QTY,
                       t_hdr_stg_to_main (w).ORDER_PRIORITY,
                       t_hdr_stg_to_main (w).PRIMARY_FC,
                       t_hdr_stg_to_main (w).QTY_REQD,
                       t_hdr_stg_to_main (w).RETAILER_NAME,
                       t_hdr_stg_to_main (w).RPT_DOMS_STATUS,
                       t_hdr_stg_to_main (w).SHIP_TO_COUNTRY,
                       t_hdr_stg_to_main (w).SHIP_TO_STATE,
                       t_hdr_stg_to_main (w).SUB_CHANNEL,
                       t_hdr_stg_to_main (w).SYSTEM_QTY,
                       t_hdr_stg_to_main (w).WORKCENTER,
                       t_hdr_stg_to_main (w).SO_ATTRIBUTE,
                       t_hdr_stg_to_main (w).LOCAL_ORDER_TYPE,
                       t_hdr_stg_to_main (w).MABD_FLAG, --Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3)
                       t_hdr_stg_to_main (w).FDD_DATE, --Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3)
                       t_hdr_stg_to_main (w).FDD_FLAG, --Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3)
                       t_hdr_stg_to_main (w).DP_ENABLED_FLAG, --Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3)
                       t_hdr_stg_to_main (w).ORIGINAL_RELEASE_DATE, --Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3)
                       t_hdr_stg_to_main (w).REVISED_RELEASE_DATE,
                       t_hdr_stg_to_main (w).ESD,
                       t_hdr_stg_to_main (w).EST_DELIVERY_DATE, --t_hdr_stg_to_main (w).
                       t_hdr_stg_to_main (w).V_P_FLAG,
                       t_hdr_stg_to_main (w).SALESREP_NAME,
                       t_hdr_stg_to_main (w).asn_3pl,
                       t_hdr_stg_to_main (w).asn_3pl_status,
                       t_hdr_stg_to_main (w).om_order_type, --<RK004>
                       t_hdr_stg_to_main (w).rsid);   --<RK004>

                 lv_insert_rec := lv_insert_rec + SQL%ROWCOUNT;
         EXCEPTION
            WHEN lv_bulk_exception
            THEN
              -- lv_error_location := 1.38;

               FOR k IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
               LOOP
                  IF t_hdr_stg_to_main.EXISTS (
                        SQL%BULK_EXCEPTIONS (k).ERROR_INDEX)
                  THEN
                     lv_indx := SQL%BULK_EXCEPTIONS (k).ERROR_INDEX;
                     lv_loop_cnt := lv_loop_cnt + 1;
                     t_excp_po (lv_loop_cnt).prod_order_num :=
                        t_hdr_stg_to_main (lv_indx).Prod_order_NUM;
                     t_excp_po (lv_loop_cnt).order_num :=
                        t_hdr_stg_to_main (lv_indx).order_num;
                     t_excp_po (lv_loop_cnt).region_code :=
                        t_hdr_stg_to_main (lv_indx).region_code;
                     t_excp_po (lv_loop_cnt).tie_number :=
                        t_hdr_stg_to_main (lv_indx).tie_number;
                     t_excp_po (lv_loop_cnt).bu_id :=
                        t_hdr_stg_to_main (lv_indx).bu_id;
                     t_excp_po (lv_loop_cnt).sys_err_code :=
                        SQL%BULK_EXCEPTIONS (k).ERROR_CODE;
                     t_excp_po (lv_loop_cnt).sys_err_mesg :=
                           SQLERRM (SQL%BULK_EXCEPTIONS (k).ERROR_CODE * -1)
                        || 'ERR LOC '
                        || lv_error_location;
                  END IF;

                  t_hdr_stg_to_main.DELETE (lv_indx);
               END LOOP;
         END;

         --VEDA CODE CHANGES BEGIN-- If one of the tie is a Precision order, the entire order is a precision order
         --- UPDATE THE HOURLY DATA WITH THE REVISED AND ORIGINAL RELEASE DATES
         BEGIN
            FORALL i IN INDICES OF t_hdr_stg_to_main SAVE EXCEPTIONS
               UPDATE scdh_reporting.OM_BACKLOG_HEADER A
                  SET V_P_FLAG = 'P'
                WHERE     ORDER_NUM = t_hdr_stg_to_main (i).ORDER_NUM
                      AND PROD_ORDER_NUM =
                             t_hdr_stg_to_main (i).PROD_ORDER_NUM
                      AND BU_ID = t_hdr_stg_to_main (i).BU_ID
                      AND TIE_NUMBER = t_hdr_stg_to_main (i).TIE_NUMBER
                      AND REGION_CODE = p_region
                      AND JOB_TIME = 'HOURLY'
                      AND MOD (ORDER_NUM, 10) = p_mod
                      AND (SELECT COUNT (V_P_FLAG)
                             FROM scdh_reporting.om_backlog_header_stg B
                            WHERE     a.order_num = B.order_num
                                  AND a.region_code = b.region_code
                                  AND a.bu_id = b.bu_id
                                  AND V_P_FLAG = 'P') >= 1;

--            FORALL i IN INDICES OF t_hdr_stg_to_main SAVE EXCEPTIONS
--               UPDATE scdh_reporting.OM_BACKLOG_HEADER A
--                  SET ORIGINAL_RELEASE_DATE =
--                         (SELECT NVL (ORIGINAL_RELEASE_DATE, SYSDATE)
--                            FROM scdh_reporting.OM_BACKLOG_HEADER B
--                           WHERE     A.ORDER_NUM = B.ORDER_NUM
--                                 AND A.REGION_CODE = B.REGION_CODE
--                                 AND A.BU_ID = B.BU_ID
--                                 AND A.PROD_ORDER_NUM = B.PROD_ORDER_NUM
--                                 AND MOD (A.ORDER_NUM, 10) = p_mod
--                                 AND A.TIE_NUMBER = B.TIE_NUMBER
--                                 AND B.JOB_TIME = '8HOURS')
--                WHERE     A.ORDER_NUM = t_hdr_stg_to_main (i).ORDER_NUM
--                      AND A.PROD_ORDER_NUM =
--                             t_hdr_stg_to_main (i).PROD_ORDER_NUM
--                      AND A.BU_ID = t_hdr_stg_to_main (i).BU_ID
--                      AND A.TIE_NUMBER = t_hdr_stg_to_main (i).TIE_NUMBER
--                      AND A.REGION_CODE = p_region
--                      AND MOD (A.ORDER_NUM, 10) = p_mod
--                      AND A.JOB_TIME = 'HOURLY'
--                      AND A.ORIGINAL_RELEASE_DATE IS NULL;
--
--            FORALL i IN INDICES OF t_hdr_stg_to_main SAVE EXCEPTIONS
--               UPDATE scdh_reporting.OM_BACKLOG_HEADER A
--                  SET REVISED_RELEASE_DATE =
--                         (SELECT NVL (REVISED_RELEASE_DATE, SYSDATE)
--                            FROM scdh_reporting.OM_BACKLOG_HEADER B
--                           WHERE     A.ORDER_NUM = B.ORDER_NUM
--                                 AND A.REGION_CODE = B.REGION_CODE
--                                 AND A.BU_ID = B.BU_ID
--                                 AND A.PROD_ORDER_NUM = B.PROD_ORDER_NUM
--                                 AND A.TIE_NUMBER = B.TIE_NUMBER
--                                 AND MOD (A.ORDER_NUM, 10) = p_mod
--                                 AND B.JOB_TIME = '8HOURS')
--                WHERE     A.ORDER_NUM = t_hdr_stg_to_main (i).ORDER_NUM
--                      AND A.PROD_ORDER_NUM =
--                             t_hdr_stg_to_main (i).PROD_ORDER_NUM
--                      AND A.BU_ID = t_hdr_stg_to_main (i).BU_ID
--                      AND A.TIE_NUMBER = t_hdr_stg_to_main (i).TIE_NUMBER
--                      AND A.REGION_CODE = p_region
--                      AND MOD (A.ORDER_NUM, 10) = p_mod
--                      AND A.JOB_TIME = 'HOURLY'
--                      AND A.REVISED_RELEASE_DATE IS NULL;
         EXCEPTION
            WHEN lv_bulk_exception
            THEN
               --lv_error_location := 1.41;

               FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
               LOOP
                  IF t_hdr_stg_to_main.EXISTS (
                        SQL%BULK_EXCEPTIONS (j).ERROR_INDEX)
                  THEN
                     lv_indx := SQL%BULK_EXCEPTIONS (j).ERROR_INDEX;
                     lv_loop_cnt := lv_loop_cnt + 1;
                     t_excp_po (lv_loop_cnt).prod_order_num :=
                        t_hdr_stg_to_main (lv_indx).Prod_order_NUM;
                     t_excp_po (lv_loop_cnt).order_num :=
                        t_hdr_stg_to_main (lv_indx).order_num;
                     t_excp_po (lv_loop_cnt).region_code :=
                        t_hdr_stg_to_main (lv_indx).region_code;
                     t_excp_po (lv_loop_cnt).tie_number :=
                        t_hdr_stg_to_main (lv_indx).tie_number;
                     t_excp_po (lv_loop_cnt).bu_id :=
                        t_hdr_stg_to_main (lv_indx).bu_id;
                     t_excp_po (lv_loop_cnt).sys_err_code :=
                        SQL%BULK_EXCEPTIONS (j).ERROR_CODE;
                     t_excp_po (lv_loop_cnt).sys_err_mesg :=
                           SQLERRM (SQL%BULK_EXCEPTIONS (j).ERROR_CODE * -1)
                        || 'ERR LOC '
                        || lv_error_location;
                  END IF;

                  t_hdr_stg_to_main.DELETE (lv_indx);
               END LOOP;
         END;

         --         BEGIN
         --            UPDATE scdh_reporting.OM_BACKLOG_HEADER A
         --               SET V_P_FLAG = 'P'
         --             WHERE (SELECT COUNT (V_P_FLAG)
         --                      FROM scdh_reporting.om_backlog_header_stg B
         --                     WHERE a.order_num = B.order_num AND V_P_FLAG = 'P') >= 1;
         --         END;

         --VEDA CODE CHANGES BEGIN-- If one of the tie is a Precision order, the entire order is a precision order


         /*----------------deleting all the orders from main table which got changed in source after last run ----------------------*/
         lv_error_location := 6.7;

         FORALL g IN INDICES OF t_hdr_stg_to_main
            UPDATE SCDH_REPORTING.OM_BACKLOG_DETAIL
               SET SYS_ENT_STATE = 'DELETED',
                   job_time = 'DELETED',
                   sys_last_modified_date = SYSTIMESTAMP
             WHERE     ORDER_NUM = t_hdr_stg_to_main (g).ORDER_NUM
                   AND PROD_ORDER_NUM = t_hdr_stg_to_main (g).PROD_ORDER_NUM
                   AND BU_ID = t_hdr_stg_to_main (g).BU_ID
                   AND TIE_NUMBER = t_hdr_stg_to_main (g).TIE_NUMBER
                   AND REGION_CODE = p_region
                   AND JOB_TIME = 'HOURLY';

         lv_error_location := 6.8;

         BEGIN
            FORALL y IN INDICES OF t_hdr_stg_to_main SAVE EXCEPTIONS
               INSERT INTO scdh_reporting.OM_BACKLOG_DETAIL (
                              PROD_ORDER_NUM,
                              PROD_ORDER_LINE,
                              ORDER_NUM,
                              BU_ID,
                              REGION_CODE,
                              TIE_NUMBER,
                              JOB_TIME,
                              SPAMS_FACILITY,
                              OVERPACK_FACILITY,
                              KITTING_FACILITY,
                              BOXING_FACILITY,
                              FAST_TRACK_CONFIG_ID,
                              ISSUE_CODE,
                              FGA_ID,
                              SKU_NUM,
                              POS_APOS,
                              TIE_GROUP,
                              MOD_PART_NUMBER,
                              PART_NUMBER,
                              CONSUMPTION_FACILITY,
                              CONSUMPTION_STATUS,
                              BACKLOG_FACILITY,
                              EMBEDDED_SS_FLAG,
                              PART_QTY,
                              QTY_STARTED,
                              FUTURE_DMD,
                              RETURN_ON_ASN,
                              PART_CONSUMPTION_FACILITY,
                              BASE_TYPE,
                              PART_TYPE_CODE,
                              RUN_DATE,
                              SYS_SOURCE,
                              SYS_CREATION_DATE,
                              SYS_LAST_MODIFIED_DATE,
                              RELEASED_DMD,
                              UNRELEASED_DMD,
                              IS_FGA_SKU,
                              --PICK_MAKE,
                              SYS_ENT_STATE,
                              BOX_CODE,
                              CONSUMED_DMD,
                              DMD_LOC_FACILITY,
                              DMS_FLAG,
                              DTL_SEQ_NUM,
                              IBU_ID,
                              ITEM_CATEGORY,
                              NET_DMD,
                              PART_TYPE_REASON_CODE,
                              QTY_EXTENDED,
                              QTY_REQD,
                              SUMMARY_DMD,
                              IS_SUPRESSED,
                              demand_supply_region_code)
                  SELECT PROD_ORDER_NUM,
                         PROD_ORDER_LINE,
                         ORDER_NUM,
                         BU_ID,
                         REGION_CODE,
                         TIE_NUMBER,
                         JOB_TIME,
                         SPAMS_FACILITY,
                         OVERPACK_FACILITY,
                         KITTING_FACILITY,
                         BOXING_FACILITY,
                         FAST_TRACK_CONFIG_ID,
                         ISSUE_CODE,
                         FGA_ID,
                         SKU_NUM,
                         POS_APOS,
                         TIE_GROUP,
                         MOD_PART_NUMBER,
                         PART_NUMBER,
                         CONSUMPTION_FACILITY,
                         CONSUMPTION_STATUS,
                         BACKLOG_FACILITY,
                         EMBEDDED_SS_FLAG,
                         PART_QTY,
                         QTY_STARTED,
                         FUTURE_DMD,
                         RETURN_ON_ASN,
                         PART_CONSUMPTION_FACILITY,
                         BASE_TYPE,
                         PART_TYPE_CODE,
                         RUN_DATE,
                         SYS_SOURCE,
                         SYS_CREATION_DATE,
                         SYS_LAST_MODIFIED_DATE,
                         RELEASED_DMD,
                         UNRELEASED_DMD,
                         IS_FGA_SKU,
                         SYS_ENT_STATE,
                         BOX_CODE,
                         CONSUMED_DMD,
                         DMD_LOC_FACILITY,
                         DMS_FLAG,
                         DTL_SEQ_NUM,
                         IBU_ID,
                         ITEM_CATEGORY,
                         NET_DMD,
                         PART_TYPE_REASON_CODE,
                         QTY_EXTENDED,
                         QTY_REQD,
                         SUMMARY_DMD,
                         is_supressed,
                         demand_supply_region_code
                    FROM scdh_reporting.om_backlog_detail_stg
                   WHERE     REGION_CODE = P_REGION
                         AND order_num = t_hdr_stg_to_main (y).order_num
                         AND PROD_order_num =
                                t_hdr_stg_to_main (y).prod_order_num
                         AND tie_number = t_hdr_stg_to_main (y).tie_number
                         AND bu_id = t_hdr_stg_to_main (y).bu_id
                         AND mod_num = t_hdr_stg_to_main (y).mod_num;

            lv_insert_rec := lv_insert_rec + SQL%ROWCOUNT;
            lv_selcted_rec := lv_selcted_rec + t_hdr_stg_to_main.COUNT;
         EXCEPTION
            WHEN lv_bulk_exception
            THEN
             --  lv_error_location := 1.40;

               FOR k IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
               LOOP
                  IF t_hdr_stg_to_main.EXISTS (
                        SQL%BULK_EXCEPTIONS (k).ERROR_INDEX)
                  THEN
                     lv_indx := SQL%BULK_EXCEPTIONS (k).ERROR_INDEX;
                     lv_loop_cnt := lv_loop_cnt + 1;
                     t_excp_po (lv_loop_cnt).prod_order_num :=
                        t_hdr_stg_to_main (lv_indx).Prod_order_NUM;
                     t_excp_po (lv_loop_cnt).order_num :=
                        t_hdr_stg_to_main (lv_indx).order_num;
                     t_excp_po (lv_loop_cnt).region_code :=
                        t_hdr_stg_to_main (lv_indx).region_code;
                     t_excp_po (lv_loop_cnt).tie_number :=
                        t_hdr_stg_to_main (lv_indx).tie_number;
                     t_excp_po (lv_loop_cnt).bu_id :=
                        t_hdr_stg_to_main (lv_indx).bu_id;
                     -- t_excp_po (lv_loop_cnt).mod_part_number :=t_hdr_stg_to_main (lv_indx).mod_part_number;
                     -- t_excp_po (lv_loop_cnt).part_number :=t_hdr_stg_to_main (lv_indx).part_number;
                     t_excp_po (lv_loop_cnt).sys_err_code :=
                        SQL%BULK_EXCEPTIONS (k).ERROR_CODE;
                     t_excp_po (lv_loop_cnt).sys_err_mesg :=
                           SQLERRM (SQL%BULK_EXCEPTIONS (k).ERROR_CODE * -1)
                        || 'ERR LOC '
                        || lv_error_location;
                  END IF;
               END LOOP;
         END;

         COMMIT;
      END LOOP;

      CLOSE cur_hdr_stg_to_main;

      ---updates the status of previous step in PROCESS_JOB_DETAIL TABLE.
      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
            P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
            P_NUMBER_OF_ROWS_INSERTED   => lv_insert_rec,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'MERGE DONE FOR HEADER AND DETAIL',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            P_ERR_CODE := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      -----------------------------------Populate OM summary----------------------------------------------
      p_insert_rec := lv_insert_rec; -- Returning count to calling block (Batch File) to process Summary.
      ----------------------------------------INSERT INTO ERR TABLE-------------------------------------------------------------
      lv_error_location := 6.9;

      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
            P_STEP_NAME                 => 'INSERT',
            P_JOB_AUDIT_MESSAGE         =>    'INSERT EXCEPTION RECORDS FOR - '
                                           || p_region
                                           || '_'
                                           || p_mod,
            P_NUMBER_OF_ROWS_SELECTED   => NULL,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_IS_BASE_TABLE             => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'INSERTION started..',
            P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
            P_JOB_INSTANCE_ID           => lv_job_instance_id,
            P_JOB_NAME                  => lv_job_name,
            P_JOB_AUDIT_LEVEL_ID        => NULL,
            P_COMMAND_NAME              => 'SQL',
            P_COMMAND_TYPE              => 'INSERT',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            p_err_code := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      lv_exception_cnt := t_excp_po.COUNT;
      lv_exEP_REC_counter := 1;
      lv_insert_rec := 0;

      BEGIN
         LOOP
            EXIT WHEN lv_exception_cnt <= 0;

            FORALL i IN lv_exEP_REC_counter .. (lv_exEP_REC_counter + 4999)
               INSERT
                 INTO scdh_audit.err_OM_backlog_details (
                         PROD_ORDER_NUM,
                         TIE_NUMBER,
                         ORDER_NUM,
                         BU_ID,
                         MOD_PART_NUMBER,
                         PART_NUMBER,
                         DTL_SEQ_NUM,
                         REGION_CODE,
                         JOB_TIME,
                         SYS_SOURCE,
                         SYS_ERR_CODE,
                         SYS_ERR_MESG,
                         SYS_NC_TYPE,
                         SYS_ENT_STATE,
                         SYS_CREATION_DATE,
                         SYS_LAST_MODIFIED_DATE)
               VALUES (t_excp_po (i).PROD_ORDER_NUM,
                       t_excp_po (i).TIE_NUMBER,
                       t_excp_po (i).ORDER_NUM,
                       t_excp_po (i).BU_ID,
                       t_excp_po (i).MOD_PART_NUMBER,
                       t_excp_po (i).PART_NUMBER,
                       t_excp_po (i).DTL_SEQ_NUM,
                       t_excp_po (i).REGION_CODE,
                       'HOURLY',
                       'SCDH',
                       t_excp_po (i).sys_err_code,
                       t_excp_po (i).sys_err_mesg,
                       'INSERT',
                       'ACTIVE',
                       SYSTIMESTAMP,
                       SYSTIMESTAMP);

            lv_insert_rec := lv_insert_rec + SQL%ROWCOUNT;
            COMMIT;
            lv_exception_cnt := lv_exception_cnt - 5000;
            lv_exEP_REC_counter := lv_exEP_REC_counter + 5000;
         END LOOP;
      EXCEPTION
         WHEN OTHERS
         THEN
            p_err_code := SQLCODE;
            p_err_mesg := SQLERRM || 'Error Location:' || lv_error_location;
      --RAISE exp_post;
      END;

      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
            P_NUMBER_OF_ROWS_SELECTED   => t_excp_po.COUNT,
            P_NUMBER_OF_ROWS_INSERTED   => lv_insert_rec,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           =>    'INSERTION DONE FOR EXCEPTION RECORDS '
                                           || p_err_mesg,
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            P_ERR_CODE := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      ----------------------------Update ACTIVE Tag To COMPLETE-----------------------------------------------
      UPDATE SCDH_AUDIT.ERR_OM_BACKLOG_DETAILS
         SET ora_err_tag$ = REPLACE (ora_err_tag$, 'ACTIVE', 'COMPLETED')
       WHERE     ora_err_tag$ LIKE '%ACTIVE%'
             AND REGION_CODE = p_region
             AND JOB_TIME = 'HOURLY'
             AND MOD_NUM = p_mod;

      COMMIT;
      ---------------------------------------------------------------------------------------------------------------------------
      /*   lv_error_location := 1.45;

         IF lv_audit_log_yn = 'Y'
         THEN
            fdl_snop_scdhub.process_audit_pkg.p_update_job_header ('COMPLETED',SYSDATE,lv_job_instance_id,lv_error_code,lv_error_msg);

            IF lv_error_code <> 0
            THEN
               lv_error_msg :=lv_error_msg || 'Err Location:' || lv_error_location;
               p_err_code := lv_error_code;
               p_err_mesg := lv_error_msg;
               RAISE exp_post;
            END IF;
         END IF;*/

      -------------------------------------------------------update start seq num with +1 for each thread-----------------------------------------
      -------------------------------------it will help to identify whether all threads completed successfully or not-------------------------------
      lv_error_location := 7.0;

      BEGIN
         OPEN cur_update_thread_status (lv_job_name);

         FETCH cur_update_thread_status INTO v_seq_num;

         UPDATE FDL_SNOP_SCDHUB.AIF_LAST_SYNC_UP
            SET START_SEQ_NUM = START_SEQ_NUM + 1
          WHERE CURRENT OF cur_update_thread_status;

         COMMIT;

         CLOSE cur_update_thread_status;

         p_err_code := C_SUCCESS_CODE;
         p_err_mesg := NULL;
      END;
   EXCEPTION
      WHEN exp_post
      THEN
         ROLLBACK;

         IF cur_get_order%ISOPEN
         THEN
            CLOSE cur_get_order;
         END IF;

         IF cur_get_order_apj%ISOPEN
         THEN
            CLOSE cur_get_order_apj;
         END IF;

         IF cur_get_order_init%ISOPEN
         THEN
            CLOSE cur_get_order_init;
         END IF;

         IF cur_hdr_stg_to_main%ISOPEN
         THEN
            CLOSE cur_hdr_stg_to_main;
         END IF;

         IF cur_delete_sync%ISOPEN
         THEN
            CLOSE cur_delete_sync;
         END IF;

         IF rc_manifest%ISOPEN
         THEN
            CLOSE rc_manifest;
         END IF;

         IF cur_update_thread_status%ISOPEN
         THEN
            CLOSE cur_update_thread_status;
         END IF;

         p_err_code := C_ERROR_CODE;
         p_err_mesg :=
               SUBSTR (SQLERRM, 1, 3500)
            || ' Err Location:'
            || lv_error_location;
      /*   fdl_snop_scdhub.process_audit_pkg.p_update_job_header ('INCOMPLETE',SYSDATE,lv_job_instance_id,lv_error_code,lv_error_msg);

          IF lv_error_code <> 0
             THEN
                lv_error_msg :=lv_error_msg || 'Err Location:' || lv_error_location;
                p_err_code := lv_error_code;
                p_err_mesg := lv_error_msg;
          END IF;

          UPDATE FDL_SNOP_SCDHUB.AIF_LAST_SYNC_UP
             SET operation_status = 'FAILURE'
           WHERE sync_up_id = lv_job_name;

          COMMIT;*/
      WHEN OTHERS
      THEN
         ROLLBACK;

         IF cur_get_order%ISOPEN
         THEN
            CLOSE cur_get_order;
         END IF;

         IF cur_hdr_stg_to_main%ISOPEN
         THEN
            CLOSE cur_hdr_stg_to_main;
         END IF;

         IF cur_get_order_apj%ISOPEN
         THEN
            CLOSE cur_get_order_apj;
         END IF;

         IF cur_get_order_init%ISOPEN
         THEN
            CLOSE cur_get_order_init;
         END IF;

         IF cur_delete_sync%ISOPEN
         THEN
            CLOSE cur_delete_sync;
         END IF;


         IF rc_manifest%ISOPEN
         THEN
            CLOSE rc_manifest;
         END IF;

         IF cur_update_thread_status%ISOPEN
         THEN
            CLOSE cur_update_thread_status;
         END IF;

         IF cur_get_FDD_OPR%ISOPEN
            THEN
               CLOSE cur_get_FDD_OPR;
         END IF;

         p_err_code := C_ERROR_CODE;
         p_err_mesg :=
               SUBSTR (SQLERRM, 1, 3500)
            || ' Err Location:'
            || lv_error_location;
   /*fdl_snop_scdhub.process_audit_pkg.p_update_job_header ('INCOMPLETE',SYSDATE,lv_job_instance_id,lv_error_code,lv_error_msg);

    IF lv_error_code <> 0
       THEN
          lv_error_msg :=lv_error_msg || 'Err Location:' || lv_error_location;
          p_err_code := lv_error_code;
          p_err_mesg := lv_error_msg;
    END IF;

    UPDATE FDL_SNOP_SCDHUB.AIF_LAST_SYNC_UP
       SET operation_status = 'FAILURE'
     WHERE sync_up_id = lv_job_name;

    COMMIT;*/
   END prc_populating_header_dtl;

   ------------------------------------OPR DATA PROCEDURE ------------------------------------------------------
   PROCEDURE prc_populating_header_dtl_opr (
      p_region        IN SCDH_FULFILLMENT.BACKLOG_HEADER.REGION_CODE%TYPE,
      p_start         IN TIMESTAMP WITH LOCAL TIME ZONE,
      p_end           IN TIMESTAMP WITH LOCAL TIME ZONE,
      p_mod           IN SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE, --
      P_OMS_SOURCE1      VARCHAR2,
      P_OMS_SOURCE2      VARCHAR2,
      P_INIT_FLAG     IN CHAR)
   IS
      --  Cursor for Initial Data Load if needs to re-process from scratch
      CURSOR cur_get_order_opr_init (
         p_region_in   IN SCDH_FULFILLMENT.BACKLOG_HEADER.REGION_CODE%TYPE,
         p_mod_in      IN NUMBER)                                           --
      IS
         SELECT DISTINCT BH.ORDER_NUMBER ORDER_NUMBER
           FROM SCDH_FULFILLMENT.BACKLOG_HEADER BH
          WHERE     BH.REGION_CODE = p_region_in -----Assumption:Taking into consideration that an order cannot span in two different regions
                AND BH.RPT_DOMS_STATUS = 'OPR'
                AND MOD (BH.ORDER_NUMBER, 10) = p_mod_in;                   --

      CURSOR cur_get_order_opr (
         p_region_in       IN SCDH_FULFILLMENT.BACKLOG_HEADER.REGION_CODE%TYPE,
         p_start_date_in   IN TIMESTAMP WITH LOCAL TIME ZONE,
         p_end_date_in     IN TIMESTAMP WITH LOCAL TIME ZONE,
         p_OMS_SOURCE1     IN VARCHAR2,
         p_OMS_SOURCE2     IN VARCHAR2,
         p_mod_in          IN NUMBER)                                       --
      IS
         SELECT DISTINCT ORDER_NUMBER
           FROM (WITH backlog_orders
                      AS (SELECT DISTINCT
                                 BH.ORDER_NUMBER ORDER_NUMBER,
                                 PROD_ORDER_NUMBER,
                                 BUID,
                                 REGION_CODE,
                                 BH.SYS_LAST_MODIFIED_DATE
                                    SYS_LAST_MODIFIED_DATE_BH
                            FROM SCDH_FULFILLMENT.BACKLOG_HEADER BH
                           WHERE     BH.REGION_CODE = p_region_in -----Assumption:Taking into consideration that an order cannot span in two different regions
                                 AND BH.RPT_DOMS_STATUS = 'OPR'
                                 AND MOD (BH.ORDER_NUMBER, 10) = p_mod_in   --
                                                                         )
                 SELECT bo.ORDER_NUMBER
                   FROM backlog_orders bo
                  WHERE bo.SYS_LAST_MODIFIED_DATE_BH BETWEEN p_start_date_in
                                                         AND p_end_date_in
                 UNION ALL
                 SELECT /*+ use_nl(backlog_orders po) */
                        SALES_ORDER_ID AS ORDER_NUMBER
                   FROM SCDH_FULFILLMENT.PROD_ORDER_OPR PO
                        INNER JOIN backlog_orders
                           ON     backlog_orders.PROD_ORDER_NUMBER =
                                     PO.PROD_ORDER_NUM
                              AND backlog_orders.REGION_CODE = PO.REGION_CODE
                  WHERE     PO.REGION_CODE = p_region_in
                        AND SYS_EXTRACT_UTC (PO.SYS_LAST_MODIFIED_DATE) BETWEEN p_start_date_in
                                                                            AND p_end_date_in
                 UNION ALL
                 SELECT BD.ORDER_NUMBER
                   FROM SCDH_FULFILLMENT.BACKLOG_DETAIL BD
                        INNER JOIN backlog_orders
                           ON     backlog_orders.ORDER_NUMBER =
                                     BD.ORDER_NUMBER
                              AND backlog_orders.BUID = BD.BUID
                              AND backlog_orders.REGION_CODE = BD.REGION_CODE
                  WHERE     BD.REGION_CODE = p_region_in
                        AND BD.SYS_LAST_MODIFIED_DATE BETWEEN p_start_date_in
                                                          AND p_end_date_in
                 UNION ALL
                 SELECT ORDER_NUM AS ORDER_NUMBER
                   FROM SCDH_FULFILLMENT.OMS_ORDHDR
                        INNER JOIN backlog_orders
                           ON     backlog_orders.ORDER_NUMBER =
                                     OMS_ORDHDR.ORDER_NUM
                              AND backlog_orders.buid = OMS_ORDHDR.bu_id
                  WHERE     OMS_ORDHDR.OMS_SOURCE IN (p_OMS_SOURCE1,
                                                      p_OMS_SOURCE2)
                        AND SYS_EXTRACT_UTC (
                               OMS_ORDHDR.SYS_LAST_MODIFIED_DATE) BETWEEN p_start_date_in
                                                                      AND p_end_date_in
                 UNION ALL
                 SELECT WO.ORDER_NUM AS ORDER_NUMBER
                   FROM BACKLOG_ORDERS
                        INNER JOIN SCDH_FULFILLMENT.WORK_ORDER WO
                           ON     BACKLOG_ORDERS.ORDER_NUMBER = WO.ORDER_NUM
                              AND BACKLOG_ORDERS.BUID = WO.BU_ID
                        INNER JOIN SCDH_FULFILLMENT.WO_MANIFEST_XREF WMF
                           ON (    WO.WO_ID = WMF.WO_ID
                               AND WO.REGION_CODE = WMF.REGION_CODE)
                        INNER JOIN SCDH_FULFILLMENT.MANIFEST MF
                           ON (    WMF.REGION_CODE = MF.REGION_CODE
                               AND WMF.MANIFEST_REF = MF.MANIFEST_REF)
                  WHERE     WO.REGION_CODE = p_region_in
                        AND mf.SYS_LAST_MODIFIED_DATE BETWEEN p_start_date_in
                                                          AND p_end_date_in
                 UNION ALL
                 SELECT so.SALES_ORDER_ID AS ORDER_NUMBER
                   FROM SCDH_FULFILLMENT.sales_order_OPR so
                        INNER JOIN backlog_orders bo
                           ON     bo.order_number = so.sales_order_id
                              AND bo.buid = SO.BUID
                              AND bo.region_code = so.region_code
                  WHERE     so.region_code = p_region_in
                        AND SO.SYS_LAST_MODIFIED_DATE BETWEEN p_start_date_in
                                                          AND p_end_date_in
                 UNION ALL
                 /* Added as part of  PPB3 to identify order with Velocity and precision*/
                 SELECT DISTINCT ORDER_NUMBER AS ORDER_NUMBER
                   FROM backlog_orders BH
                        INNER JOIN SCDH_FULFILLMENT.MFG_ORD_BKLG_VP BVP
                           ON     bh.order_number = BVP.ORD_NUM
                              AND bh.buid = BVP.BU_ID
                              AND bh.region_code = BVP.region
                              AND BVP.REGION = p_region_in
                              AND BVP.SYS_LAST_MODIFIED_DATE BETWEEN p_start_date_in
                                                                 AND p_end_date_in
                              INNER JOIN
                                 SCDH_REPORTING.OM_BACKLOG_HEADER OMH
                                 ON OMH.ORDER_NUM=BVP.ORD_NUM
                                         AND OMH.BU_ID = BVP.BU_ID
                                         AND OMH.REGION_CODE = BVP.REGION
                                         AND trunc(OMH.TIE_NUMBER)=trunc(BVP.ORD_TIE_NUM)
                                         AND V_P_FLAG<>VEL_PRECISON_FLAG
                                         AND JOB_TIME='HOURLY'
                                         AND BVP.REGION = p_region_in
                 UNION ALL
                 /* Added as part of  PPB3 to identify order from the lead time table*/
                 SELECT DISTINCT bh.ORDER_NUMBER AS ORDER_NUMBER
                   FROM backlog_orders BH
                        INNER JOIN SCDH_FULFILLMENT.SO_LEAD_TIME SOLT
                           ON     bh.order_number = SOLT.SO_NBR
                              AND bh.buid = SOLT.BU_ID
                               AND SOLT.VER_NUM=1
                              AND bh.region_code = SOLT.DELL_RGN_CD
                              AND SOLT.DELL_RGN_CD = p_region_in
                              AND SOLT.SYS_LAST_MODIFIED_DATE BETWEEN p_start_date_in
                                                                  AND p_end_date_in);


      TYPE coll_cur_get_order IS TABLE OF cur_get_order_opr%ROWTYPE;

      t_cur_get_order       coll_cur_get_order;

      CURSOR cur_get_order_opr_apj (
         p_region_in       IN SCDH_FULFILLMENT.BACKLOG_HEADER.REGION_CODE%TYPE,
         p_start_date_in   IN TIMESTAMP WITH LOCAL TIME ZONE,
         p_end_date_in     IN TIMESTAMP WITH LOCAL TIME ZONE,
         p_OMS_SOURCE1     IN VARCHAR2,
         p_OMS_SOURCE2     IN VARCHAR2,
         p_mod_in          IN SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE) --
      IS
         SELECT DISTINCT ORDER_NUMBER
           FROM (WITH backlog_orders
                      AS (SELECT DISTINCT
                                 BH.ORDER_NUMBER ORDER_NUMBER,
                                 PROD_ORDER_NUMBER,
                                 BUID,
                                 REGION_CODE,
                                 BH.SYS_LAST_MODIFIED_DATE
                                    SYS_LAST_MODIFIED_DATE_BH
                            FROM SCDH_FULFILLMENT.BACKLOG_HEADER BH
                           WHERE     BH.REGION_CODE = p_region_in -----Assumption:Taking into consideration that an order cannot span in two different regions
                                 AND BH.RPT_DOMS_STATUS = 'OPR'
                                 AND MOD (BH.ORDER_NUMBER, 10) = p_mod_in   --
                                                                         )
                 SELECT bo.ORDER_NUMBER
                   FROM backlog_orders bo
                  WHERE bo.SYS_LAST_MODIFIED_DATE_BH BETWEEN p_start_date_in
                                                         AND p_end_date_in
                 UNION ALL
                 SELECT /*+ ordered use_nl(backlog_orders po) */
                        SALES_ORDER_ID AS ORDER_NUMBER
                   FROM SCDH_FULFILLMENT.PROD_ORDER_OPR PO
                        INNER JOIN backlog_orders
                           ON     backlog_orders.PROD_ORDER_NUMBER =
                                     PO.PROD_ORDER_NUM
                              AND backlog_orders.REGION_CODE = PO.REGION_CODE
                  WHERE     PO.REGION_CODE = p_region_in
                        AND SYS_EXTRACT_UTC (PO.SYS_LAST_MODIFIED_DATE) BETWEEN p_start_date_in
                                                                            AND p_end_date_in
                 UNION ALL
                 SELECT BD.ORDER_NUMBER
                   FROM SCDH_FULFILLMENT.BACKLOG_DETAIL BD
                        INNER JOIN backlog_orders
                           ON     backlog_orders.ORDER_NUMBER =
                                     BD.ORDER_NUMBER
                              AND backlog_orders.BUID = BD.BUID
                              AND backlog_orders.REGION_CODE = BD.REGION_CODE
                  WHERE     BD.REGION_CODE = p_region_in
                        AND BD.SYS_LAST_MODIFIED_DATE BETWEEN p_start_date_in
                                                          AND p_end_date_in
                 UNION ALL
                 SELECT /*+ ordered use_nl(backlog_orders oms_ordhdr) */
                        ORDER_NUM AS ORDER_NUMBER
                   FROM SCDH_FULFILLMENT.OMS_ORDHDR
                        INNER JOIN backlog_orders
                           ON     backlog_orders.ORDER_NUMBER =
                                     OMS_ORDHDR.ORDER_NUM
                              AND backlog_orders.buid = OMS_ORDHDR.bu_id
                  WHERE     OMS_ORDHDR.OMS_SOURCE IN (p_OMS_SOURCE1,
                                                      p_OMS_SOURCE2)
                        AND SYS_EXTRACT_UTC (
                               OMS_ORDHDR.SYS_LAST_MODIFIED_DATE) BETWEEN p_start_date_in
                                                                      AND p_end_date_in
                 UNION ALL
                 SELECT /*+ use_nl(WO mf wmf )*/
                        WO.ORDER_NUM AS ORDER_NUMBER
                   FROM BACKLOG_ORDERS
                        INNER JOIN SCDH_FULFILLMENT.WORK_ORDER WO
                           ON     BACKLOG_ORDERS.ORDER_NUMBER = WO.ORDER_NUM
                              AND BACKLOG_ORDERS.BUID = WO.BU_ID
                        INNER JOIN SCDH_FULFILLMENT.WO_MANIFEST_XREF WMF
                           ON (    WO.WO_ID = WMF.WO_ID
                               AND WO.REGION_CODE = WMF.REGION_CODE)
                        INNER JOIN SCDH_FULFILLMENT.MANIFEST MF
                           ON (    WMF.REGION_CODE = MF.REGION_CODE
                               AND WMF.MANIFEST_REF = MF.MANIFEST_REF)
                  WHERE     WO.REGION_CODE = p_region_in
                        AND mf.SYS_LAST_MODIFIED_DATE BETWEEN p_start_date_in
                                                          AND p_end_date_in
                 UNION ALL
                 SELECT /*+ ordered use_NL(SO BO) INDEX(SO SALES_ORDER_PK) */
                        so.SALES_ORDER_ID AS ORDER_NUMBER
                   FROM SCDH_FULFILLMENT.sales_order_OPR so
                        INNER JOIN backlog_orders bo
                           ON     bo.order_number = so.sales_order_id
                              AND bo.buid =
                                     (SELECT DECODE (
                                                v_ibu_id_flag,
                                                'Y', NVL (
                                                        (SELECT DISTINCT
                                                                BU_ID
                                                           FROM Scdh_master.MST_IBU_BUID_XREF
                                                          WHERE IBU_ID =
                                                                   SO.BUID),
                                                        so.buid),
                                                SO.BUID)
                                        FROM DUAL)
                              AND bo.region_code = so.region_code
                  WHERE     SO.REGION_CODE = p_region_in
                        AND SO.SYS_LAST_MODIFIED_DATE BETWEEN p_start_date_in
                                                          AND p_end_date_in
                 UNION ALL
                 /* Added as part of  PPB3 to identify order with Velocity and precision*/
                 SELECT DISTINCT ORDER_NUMBER AS ORDER_NUMBER
                   FROM backlog_orders BH
                        INNER JOIN SCDH_FULFILLMENT.MFG_ORD_BKLG_VP BVP
                           ON     bh.order_number = BVP.ORD_NUM
                              AND bh.buid = BVP.BU_ID
                              AND bh.region_code = BVP.region
                              AND BVP.REGION = p_region_in
                              AND BVP.SYS_LAST_MODIFIED_DATE BETWEEN p_start_date_in
                                                                 AND p_end_date_in
                        INNER JOIN
                                 SCDH_REPORTING.OM_BACKLOG_HEADER OMH
                                 ON OMH.ORDER_NUM=BVP.ORD_NUM
                                         AND OMH.BU_ID = BVP.BU_ID
                                         AND OMH.REGION_CODE = BVP.REGION
                                         AND trunc(OMH.TIE_NUMBER)=trunc(BVP.ORD_TIE_NUM)
                                         AND V_P_FLAG<>VEL_PRECISON_FLAG
                                         AND JOB_TIME='HOURLY'
                                         AND BVP.REGION = p_region_in
                 UNION ALL
                 /* Added as part of  PPB3 to identify order from the lead time table*/
                 SELECT DISTINCT bh.ORDER_NUMBER AS ORDER_NUMBER
                   FROM backlog_orders BH
                        INNER JOIN SCDH_FULFILLMENT.SO_LEAD_TIME SOLT
                           ON     bh.order_number = SOLT.SO_NBR
                              AND bh.buid = SOLT.BU_ID
                               AND SOLT.VER_NUM=1
                              AND bh.region_code = SOLT.DELL_RGN_CD
                              AND SOLT.DELL_RGN_CD = p_region_in
                              AND SOLT.SYS_LAST_MODIFIED_DATE BETWEEN p_start_date_in
                                                                  AND p_end_date_in);


      --                                                   UNION ALL
      --                                                      /* Added as part of Merge Backlog requirement*/
      --          select  ORDER_NUM AS ORDER_NUMBER FROM  backlog_orders BH
      --                INNER JOIN SCDH_REPORTING.STG_DRGN_TNT SDT
      --                ON  bh.order_number=SDT.ORDER_NUM
      --                   WHERE SDT.REGION=p_region_in
      --          AND SDT.SYS_LAST_MODIFIED_DATE BETWEEN p_start_date_in
      --                                                 AND p_end_date_in);

      -- Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3) begin

      TYPE coll_cur_get_order_apj IS TABLE OF cur_get_order_opr_apj%ROWTYPE;

      t_cur_get_order_apj   coll_cur_get_order_apj;

/* commented as this cursor is not used in the execution block
      CURSOR Cur_get_FDD_APJ (
         p_region    scdh_REPORTING.OM_BACKLOG_HEADER_STG.region_code%TYPE,
         p_mod       SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE)
      IS
         SELECT bh.ORDER_NUM,
                bh.MUST_ARRIVE_BY_DATE,
                CHARTOROWID (BH.ROWID) HDR_ROW,
                fdd.FDD_FLAG,
                fdd.FDD_DATE
           FROM scdh_REPORTING.OM_BACKLOG_HEADER_STG bh
                JOIN scdh_fulfillment.STG_DRGN_ORD_HEADER fdd
                   ON fdd.ORDER_NUM = bh.ORDER_NUM
          WHERE     BH.region_code = p_region
                AND BH.JOB_TIME = 'HOURLY'
                AND BH.MOD_NUM = p_mod;

      TYPE coll_FDD_APJ IS TABLE OF Cur_get_FDD_APJ%ROWTYPE;

      t_FDD_APJ             coll_FDD_APJ;



      CURSOR Cur_get_FDD (
         p_region    scdh_REPORTING.OM_BACKLOG_HEADER_STG.region_code%TYPE,
         p_mod       SCDH_REPORTING.OM_BACKLOG_HEADER_STG.MOD_NUM%TYPE)
      IS
         SELECT DISTINCT order_num,
                         CHARTOROWID (hd.ROWID) HDR_ROW,
                         hd.MUST_ARRIVE_BY_DATE MUST_ARRIVE_BY_DATE,
                         so.ESD ESD
           FROM scdh_reporting.om_backlog_header_stg hd
                INNER JOIN scdh_outbound.sales_order_vw so
                   ON     hd.order_num = so.sales_order_id
                      AND hd.bu_id = SO.buid
                      AND hd.region_code = so.region_code
                      AND hd.job_time = 'HOURLY'
                INNER JOIN scdh_outbound.PROD_ORDER_VW PO
                   ON     SO.SALES_ORDER_ID = PO.SALES_ORDER_ID
                      AND SO.BUID = PO.BUID
                      AND SO.REGION_CODE = PO.REGION_CODE
                INNER JOIN scdh_outbound.PROD_ORDER_LINE_VW POL
                   ON     PO.PROD_ORDER_NUM = POL.PROD_ORDER_NUM
                      AND PO.REGION_CODE = POL.REGION_CODE
                INNER JOIN scdh_outbound.LINE_SKU_VW LS
                   ON     POL.PROD_ORDER_NUM = LS.PROD_ORDER_NUM
                      AND POL.LINE_NUM = LS.LINE_NUM
                      AND ls.qty > 0
                      AND (   LS.mfg_part_num IN (SELECT SKU_MOD_NBR
                                                    FROM SCDH_MASTER.FDD_SKU_MOD)
                           OR LS.SKU_NUM IN (SELECT SKU_MOD_NBR
                                               FROM SCDH_MASTER.FDD_SKU_MOD))
          WHERE hd.MOD_NUM = p_mod AND hd.region_code = p_region;

      TYPE coll_FDD IS TABLE OF Cur_get_FDD%ROWTYPE;

      t_FDD                 coll_FDD;

*/ -- commented part end

      --Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3) end

      t_get_order_tab_opr   SCDH_REPORTING.TAB_VARCHAR;
      VAR_ERR_CODE          VARCHAR2 (4000) := 0;
      VAR_ERR_MSG           VARCHAR2 (4000);
      --V_ORDER_SOURCE              scdh_reporting.OM_BACKLOG_HEADER.ORDER_SOURCE%TYPE;
      lv_selcted_rec        NUMBER := 0;
      lv_insert_rec         NUMBER := 0;
   BEGIN
      lv_error_location := 7.1;

      -------------------------------------------------------------------------------------------------------------------------------
      IF cur_get_order_opr%ISOPEN
      THEN
         CLOSE cur_get_order_opr;
      END IF;

      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
            P_STEP_NAME                 => 'INSERT',
            P_JOB_AUDIT_MESSAGE         =>    'INSERTING OPR ORDER INTO OM BACKLOG HEADER STG FOR - '
                                           || p_region
                                           || '_'
                                           || p_mod,
            P_NUMBER_OF_ROWS_SELECTED   => NULL,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_IS_BASE_TABLE             => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'Insertion started..',
            P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
            P_JOB_INSTANCE_ID           => lv_job_instance_id,
            P_JOB_NAME                  => lv_job_name,
            P_JOB_AUDIT_LEVEL_ID        => NULL,
            P_COMMAND_NAME              => 'SQL',
            P_COMMAND_TYPE              => 'INSERT',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => VAR_ERR_CODE,
            P_ERROR_MSG                 => VAR_ERR_MSG);
      END IF;

      ------------------------OPEN CURSOR TO GET THE MODIFIED ORDERS AFTER LAST SYNC UP DATE--------------------------------
      IF P_INIT_FLAG = 'Y'
      THEN
         OPEN cur_get_order_opr_init (p_region, p_mod);                     --
      ELSE
         IF p_region = 'APJ'
         THEN
            OPEN cur_get_order_opr_apj (p_region,
                                        p_start,
                                        p_end,
                                        P_OMS_SOURCE1,
                                        P_OMS_SOURCE2,
                                        p_mod);
         ELSE
            OPEN cur_get_order_opr (p_region,
                                    p_start,
                                    p_end,
                                    P_OMS_SOURCE1,
                                    P_OMS_SOURCE2,
                                    p_mod);
         END IF;
      END IF;

      LOOP
         IF P_INIT_FLAG = 'Y'
         THEN
            FETCH cur_get_order_opr_init
               BULK COLLECT INTO t_get_order_tab_opr
               LIMIT lv_limit2;
         ELSE
            IF p_region = 'APJ'
            THEN
               FETCH cur_get_order_opr_apj
                  BULK COLLECT INTO t_get_order_tab_opr
                  LIMIT lv_limit2;


               FETCH cur_get_order_opr_apj
                  BULK COLLECT INTO t_cur_get_order_apj
                  LIMIT lv_limit2;
            ELSE
               FETCH cur_get_order_opr
                  BULK COLLECT INTO t_get_order_tab_opr
                  LIMIT lv_limit2;


               FETCH cur_get_order_opr
                  BULK COLLECT INTO t_cur_get_order
                  LIMIT lv_limit2;
            END IF;
         END IF;

         EXIT WHEN t_get_order_tab_opr.COUNT = 0;

         --------------------------MOVE DATA TO HEADER STAG TABLE ------------------------------------------
         IF p_region = 'APJ'
         THEN
            INSERT INTO SCDH_REPORTING.OM_BACKLOG_HEADER_STG (
                           PROD_ORDER_NUM,
                           PROD_ORDER_LINE,
                           ORDER_NUM,
                           BU_ID,
                           REGION_CODE,
                           TIE_NUMBER,
                           PO_NUM,
                           JOB_TIME,
                           QUOTE_NUMBER,
                           OMS_DOMS_STATUS,
                           DOMS_STATUS,
                           CYCLE_STAGE_CODE,
                           CYCLE_SUBSTAGE_CODE,
                           CYCLE_DATE,
                           SSC_NAME,
                           SS_TYPE_NAME,
                           MERGE_TYPE,
                           ORDER_TYPE,
                           BACKLOG_TYPE,
                           sub_rgn_cde,
                           SOURCE_LOCAL_CHANNEL_CODE,
                           ORDER_DATE,
                           CANCEL_DATE,
                           IS_RETAIL,
                           SHIP_BY_DATE,
                           MUST_ARRIVE_BY_DATE,
                           CFI_FLAG,
                           TP_FACILITY,
                           SHIP_TO_FACILITY,
                           PLANNED_MERGE_FACILITY,
                           CUSTOMER_PICK_UP_FLAG,
                           SHIP_METHOD_CODE,
                           SHIP_MODE_CODE,
                           SHIP_CODE,
                           DIRECT_SHIP_FLAG,
                           SYSTEM_TYPE_CODE,
                           BASE_PROD_PROT_ID,
                           BASE_PROD_CODE,
                           FMLY_PARNT_NAME,
                           WORK_CENTER,
                           BASE_SKU,
                           base_type,
                           ccn,
                           CUSTOMER_NUM,
                           CUSTOMER_NAME,
                           RUSH_FLAG,
                           BUILD_TYPE,
                           ASN_LIFE_CYCLE_SUB_STATUS,
                           OFS_STATUS,
                           BACKLOG_ORDER_TIE_TYPE,
                           RUN_DATE,
                           SYS_SOURCE,
                           SYS_CREATION_DATE,
                           SYS_LAST_MODIFIED_DATE,
                           FULF_CHANNEL,
                           IBU_ID,
                           IP_DATE,
                           ITEM_QTY,
                           ORDER_PRIORITY,
                           PRIMARY_FC,
                           QTY_REQD,
                           RETAILER_NAME,
                           RPT_DOMS_STATUS,
                           SHIP_TO_COUNTRY,
                           SHIP_TO_STATE,
                           SUB_CHANNEL,
                           SYSTEM_QTY,
                           WORKCENTER,
                           sys_ent_state,
                           -- Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3) begin
                           --MABD_FLAG,
                           DP_ENABLED_FLAG,
                           ORIGINAL_RELEASE_DATE,
                           REVISED_RELEASE_DATE,
                           V_P_FLAG,
                           ESD,
                           EST_DELIVERY_DATE,
                           -- Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3) end
                           SALESREP_NAME,
                           mod_num
                           )
               (SELECT /*+ index(bh BKLG_HDR_IDX) use_nl (bh so po pol om_hd) */
                      BH.PROD_ORDER_NUMBER,
                       BH.PROD_ORDER_LINE,
                       BH.ORDER_NUMBER,
                       BH.BUID BU_ID,
                       BH.REGION_CODE REGION_CODE,
                       BH.TIE_NUMBER TIE_NUMBER,
                       NVL (SO.PURCHASE_ORDER_NUM, 'UNK') PO_NUM,
                       'HOURLY' JOB_TIME,
                       'UNK',
                       OM_HD.CURRENT_STATUS_CODE OMS_DOMS_STATUS,
                       BH.DOMS_STATUS DOMS_STATUS,
                       'UNK',
                       'UNK',
                       CASE
                          WHEN OM_HD.CURRENT_STATUS_CODE = 'OPR'
                          THEN
                             OM_HD.ORDER_DATE
                          WHEN OM_HD.CURRENT_STATUS_CODE = 'IP'
                          THEN
                             OM_HD.IN_PROD_DATETIME
                          WHEN OM_HD.CURRENT_STATUS_CODE = 'PP'
                          THEN
                             OM_HD.PEND_PROD_DATETIME
                          WHEN OM_HD.CURRENT_STATUS_CODE = 'MN'
                          THEN
                             OM_HD.SHIPPED_DATETIME
                          WHEN OM_HD.CURRENT_STATUS_CODE = 'IN'
                          THEN
                             OM_HD.INVOICE_DATE
                          ELSE
                             OM_HD.CURRENT_STATUS_DATETIME
                       END
                          CYCLE_DATE,
                       BH.SSC_NAME SSC_NAME,
                       'UNK' SS_TYPE_NAME,
                       PO.MERGE_TYPE MERGE_TYPE,
                       OM_HD.ORDER_TYPE_CODE ORDER_TYPE,
                       'BACKLOG_TYPE',
                       NULL,
                       CASE -----------Local Channel for EMEA and APJ. AMER is fetched from get_ai_doms_ord function ------------------
                          WHEN BH.REGION_CODE = 'EMEA'
                          THEN
                             DECODE (so.buid,
                                     '5458', so.sales_channel_code,
                                     SUBSTR (so.sales_channel_code, 4))
                          WHEN BH.REGION_CODE = 'APJ'
                          THEN
                             NVL (
                                SUBSTR (so.cost_center,
                                        INSTR (so.cost_center, '_') + 1),
                                SUBSTR (
                                   so.sales_channel_code,
                                   INSTR (so.sales_channel_code, '_') + 1))
                          WHEN BH.REGION_CODE = 'AMER'
                          THEN
                             DECODE (so.buid,
                                     '11', so.company_num,
                                     NVL (so.customer_class, so.company_num))
                       END
                          SOURCE_LOCAL_CHANNEL_CODE,
                       NVL (SO.ORDER_DATE, BH.order_date) ORDER_DATE,
                       OM_HD.cancel_date CANCEL_DATE,
                       PO.IS_RETAIL IS_RETAIL,
                       BH.SHIP_BY_DATE SHIP_BY_DATE,
                       SO.MUST_ARRIVE_BY_DATE,
                       CASE WHEN POL.SI_NUMBER <> '0' THEN 'Y' ELSE 'N' END
                          CFI_FLAG,
                       BH.TP_FACILITY TP_FACILITY,
                       NVL (BH.SHIP_TO_FACILITY, po.ship_to_facility)
                          SHIP_TO_FACILITY,
                       BH.Ship_To_Facility PLANNED_MERGE_FACILITY,
                       fn_get_pickup_flag (BH.SHIP_CODE)
                          CUSTOMER_PICK_UP_FLAG,
                       'UNK',
                       PO.SHIP_MODE SHIP_MODE_CODE,
                       BH.SHIP_CODE SHIP_CODE,
                       NULL,
                       'SYSTEM_TYPE_CODE',
                       NULL,
                       NULL,
                       NULL,
                       POL.WORK_CENTER WORK_CENTER,
                       BH.BASE_SKU,
                       BH.base_type,
                       BH.CCN,
                       SO.CUSTOMER_NUM CUSTOMER_NUM,
                       SO.CUSTOMER_NAME CUSTOMER_NAME,
                       OM_HD.Rush_Flag RUSH_FLAG,
                       PO.BUILD_TYPE BUILD_TYPE,
                       NULL,
                       BH.OFS_STATUS OFS_STATUS,
                       NULL,
                       SYSDATE,
                       'FDL',
                       SYSDATE,
                       SYSDATE,
                       BH.FULF_CHANNEL,
                       BH.IBU_ID,
                       BH.IP_DATE,
                       BH.ITEM_QTY,
                       BH.ORDER_PRIORITY,
                       BH.PRIMARY_FC,
                       BH.QTY_REQD,
                       BH.RETAILER_NAME,
                       BH.RPT_DOMS_STATUS,
                       BH.SHIP_TO_COUNTRY,
                       BH.SHIP_TO_STATE,
                       BH.SUB_CHANNEL,
                       BH.SYSTEM_QTY,
                       BH.WORKCENTER,
                       'ACTIVE',
                       --Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3) begin
                       /*CASE
                          WHEN NVL (SO.MUST_ARRIVE_BY_DATE,
                                    TO_DATE ('1/1/0001', 'DD/MM/YYYY')) <
                                  TO_DATE ('1/1/2000', 'DD/MM/YYYY')
                          THEN
                             'N'
                          -- WHEN TO_DATE ('1/1/1900', 'DD/MM/YYYY') THEN 'N'
                          ELSE
                             'Y'
                       END
                          MABD_FLAG,*/ --commented above code as part of Story#6741082 
                       'N',
                       NULL,
                       NULL,
                       NVL (VEL_PRECISON_FLAG, 'V'),
                       SO.ESD,
                       SO.EST_DELIVERY_DATE,
                       --Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3) end
                       OM_HD.SALESREP_NAME,
                       p_mod
                    FROM TABLE (t_get_order_tab_opr) T_ORDER
                       INNER JOIN SCDH_FULFILLMENT.BACKLOG_HEADER BH
                          ON T_ORDER.COLUMN_VALUE = BH.ORDER_NUMBER
                       LEFT JOIN SCDH_FULFILLMENT.SALES_ORDER_OPR SO
                          ON     BH.ORDER_NUMBER = SO.SALES_ORDER_ID
                             AND BH.REGION_CODE = SO.REGION_CODE
                             AND BH.BUID =
                                    (SELECT DECODE (
                                               v_ibu_id_flag,
                                               'Y', NVL (
                                                       (SELECT DISTINCT BU_ID
                                                          FROM Scdh_master.MST_IBU_BUID_XREF
                                                         WHERE IBU_ID =
                                                                  SO.BUID),
                                                       so.buid),
                                               SO.BUID)
                                       FROM DUAL)
                       LEFT JOIN SCDH_FULFILLMENT.PROD_ORDER_OPR PO
                          ON (    PO.PROD_ORDER_NUM = BH.PROD_ORDER_NUMBER
                              AND PO.REGION_CODE = BH.REGION_CODE)
                       LEFT JOIN SCDH_FULFILLMENT.PROD_ORDER_LINE_OPR POL
                          ON (    BH.PROD_ORDER_NUMBER = POL.PROD_ORDER_NUM
                              AND BH.REGION_CODE = POL.REGION_CODE
                              AND BH.PROD_ORDER_LINE = POL.LINE_NUM)
                       LEFT JOIN SCDH_FULFILLMENT.OMS_ORDHDR OM_HD
                          ON (    OM_HD.ORDER_NUM = BH.ORDER_NUMBER
                              AND OM_HD.BU_ID = BH.BUID
                              AND OM_HD.OMS_SOURCE IN (p_OMS_SOURCE1,
                                                       p_OMS_SOURCE2))
                       LEFT OUTER JOIN SCDH_FULFILLMENT.MFG_ORD_BKLG_VP BVP
                          ON     bh.order_number = BVP.ORD_NUM
                             AND bh.buid = BVP.BU_ID
                             AND TRUNC (bh.TIE_NUMBER) =
                                    TRUNC (BVP.ORD_TIE_NUM)
                             AND bh.region_code = BVP.region
                 WHERE     BH.REGION_CODE = p_region --AND ORDER_NUMBER = p_order_num
                       AND BH.RPT_DOMS_STATUS = 'OPR')
                    LOG ERRORS INTO SCDH_AUDIT.ERR_OM_BACKLOG_DETAILS
                           ('INSERT OPR HEADER_STG-ACTIVE')
                           REJECT LIMIT UNLIMITED;
         ELSE
            INSERT INTO SCDH_REPORTING.OM_BACKLOG_HEADER_STG (
                           PROD_ORDER_NUM,
                           PROD_ORDER_LINE,
                           ORDER_NUM,
                           BU_ID,
                           REGION_CODE,
                           TIE_NUMBER,
                           PO_NUM,
                           JOB_TIME,
                           QUOTE_NUMBER,
                           OMS_DOMS_STATUS,
                           DOMS_STATUS,
                           CYCLE_STAGE_CODE,
                           CYCLE_SUBSTAGE_CODE,
                           CYCLE_DATE,
                           SSC_NAME,
                           SS_TYPE_NAME,
                           MERGE_TYPE,
                           ORDER_TYPE,
                           BACKLOG_TYPE,
                           SUB_RGN_CDE,
                           SOURCE_LOCAL_CHANNEL_CODE,
                           ORDER_DATE,
                           CANCEL_DATE,
                           IS_RETAIL,
                           SHIP_BY_DATE,
                           MUST_ARRIVE_BY_DATE,
                           CFI_FLAG,
                           TP_FACILITY,
                           SHIP_TO_FACILITY,
                           PLANNED_MERGE_FACILITY,
                           CUSTOMER_PICK_UP_FLAG,
                           SHIP_METHOD_CODE,
                           SHIP_MODE_CODE,
                           SHIP_CODE,
                           DIRECT_SHIP_FLAG,
                           SYSTEM_TYPE_CODE,
                           BASE_PROD_PROT_ID,
                           BASE_PROD_CODE,
                           FMLY_PARNT_NAME,
                           WORK_CENTER,
                           BASE_SKU,
                           base_type,
                           ccn,
                           CUSTOMER_NUM,
                           CUSTOMER_NAME,
                           RUSH_FLAG,
                           BUILD_TYPE,
                           ASN_LIFE_CYCLE_SUB_STATUS,
                           OFS_STATUS,
                           BACKLOG_ORDER_TIE_TYPE,
                           RUN_DATE,
                           SYS_SOURCE,
                           SYS_CREATION_DATE,
                           SYS_LAST_MODIFIED_DATE,
                           FULF_CHANNEL,
                           IBU_ID,
                           IP_DATE,
                           ITEM_QTY,
                           ORDER_PRIORITY,
                           PRIMARY_FC,
                           QTY_REQD,
                           RETAILER_NAME,
                           RPT_DOMS_STATUS,
                           SHIP_TO_COUNTRY,
                           SHIP_TO_STATE,
                           SUB_CHANNEL,
                           SYSTEM_QTY,
                           WORKCENTER,
                           sys_ent_state,
                           --  Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3) begin
                           MABD_FLAG,
                           DP_ENABLED_FLAG,
                           ORIGINAL_RELEASE_DATE,
                           REVISED_RELEASE_DATE,
                           V_P_FLAG,
                           ESD,
                           EST_DELIVERY_DATE,
                           -- Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3) end
                           LOCAL_ORDER_TYPE,
                           SALESREP_NAME,
                           mod_num,
                           asn_3pl,
                           asn_3pl_status,
                           om_order_type,--<RK004>
                           rsid --<RK004>
                           )
               (SELECT /*+ index(bh BKLG_HDR_IDX) use_nl (bh so po pol om_hd) */
                      BH.PROD_ORDER_NUMBER,
                       BH.PROD_ORDER_LINE,
                       BH.ORDER_NUMBER,
                       BH.BUID BU_ID,
                       BH.REGION_CODE REGION_CODE,
                       BH.TIE_NUMBER TIE_NUMBER,
                       NVL (SO.PURCHASE_ORDER_NUM, 'UNK') PO_NUM,
                       'HOURLY' JOB_TIME,
                       'UNK',
                       OM_HD.CURRENT_STATUS_CODE OMS_DOMS_STATUS,
                       BH.DOMS_STATUS DOMS_STATUS,
                       'UNK',
                       'UNK',
                       CASE
                          WHEN OM_HD.CURRENT_STATUS_CODE = 'OPR'
                          THEN
                             OM_HD.ORDER_DATE
                          WHEN OM_HD.CURRENT_STATUS_CODE = 'IP'
                          THEN
                             OM_HD.IN_PROD_DATETIME
                          WHEN OM_HD.CURRENT_STATUS_CODE = 'PP'
                          THEN
                             OM_HD.PEND_PROD_DATETIME
                          WHEN OM_HD.CURRENT_STATUS_CODE = 'MN'
                          THEN
                             OM_HD.SHIPPED_DATETIME
                          WHEN OM_HD.CURRENT_STATUS_CODE = 'IN'
                          THEN
                             OM_HD.INVOICE_DATE
                          ELSE
                             OM_HD.CURRENT_STATUS_DATETIME
                       END
                          CYCLE_DATE,
                       BH.SSC_NAME SSC_NAME,
                       'UNK' SS_TYPE_NAME,
                       PO.MERGE_TYPE MERGE_TYPE,
                       OM_HD.ORDER_TYPE_CODE ORDER_TYPE,
                       'BACKLOG_TYPE',
                       NULL,
                       CASE -----------Local Channel for EMEA and APJ. AMER is fetched from get_ai_doms_ord function ------------------
                          WHEN BH.REGION_CODE = 'EMEA'
                          THEN
                             DECODE (so.buid,
                                     '5458', so.sales_channel_code,
                                     SUBSTR (so.sales_channel_code, 4))
                          WHEN BH.REGION_CODE = 'APJ'
                          THEN
                             NVL (
                                SUBSTR (so.cost_center,
                                        INSTR (so.cost_center, '_') + 1),
                                SUBSTR (
                                   so.sales_channel_code,
                                   INSTR (so.sales_channel_code, '_') + 1))
                          WHEN BH.REGION_CODE = 'AMER'
                          THEN
                             DECODE (so.buid,
                                     '11', so.company_num,
                                     NVL (so.customer_class, so.company_num))
                       END
                          SOURCE_LOCAL_CHANNEL_CODE,
                       NVL (SO.ORDER_DATE, BH.order_date) ORDER_DATE,
                       OM_HD.cancel_date CANCEL_DATE,
                       PO.IS_RETAIL IS_RETAIL,
                       BH.SHIP_BY_DATE SHIP_BY_DATE,
                       SO.MUST_ARRIVE_BY_DATE,
                       CASE WHEN POL.SI_NUMBER <> '0' THEN 'Y' ELSE 'N' END
                          CFI_FLAG,
                       BH.TP_FACILITY TP_FACILITY,
                       NVL (BH.SHIP_TO_FACILITY, po.ship_to_facility)
                          SHIP_TO_FACILITY,
                       BH.Ship_To_Facility PLANNED_MERGE_FACILITY,
                       fn_get_pickup_flag (BH.SHIP_CODE)
                          CUSTOMER_PICK_UP_FLAG,
                       'UNK',
                       PO.SHIP_MODE SHIP_MODE_CODE,
                       BH.SHIP_CODE SHIP_CODE,
                       NULL,
                       'SYSTEM_TYPE_CODE',
                       NULL,
                       NULL,
                       NULL,
                       POL.WORK_CENTER WORK_CENTER,
                       BH.BASE_SKU,
                       BH.base_type,
                       BH.CCN,
                       SO.CUSTOMER_NUM CUSTOMER_NUM,
                       SO.CUSTOMER_NAME CUSTOMER_NAME,
                       OM_HD.Rush_Flag RUSH_FLAG,
                       PO.BUILD_TYPE BUILD_TYPE,
                       NULL,
                       BH.OFS_STATUS OFS_STATUS,
                       NULL,
                       SYSDATE,
                       'FDL',
                       SYSDATE,
                       SYSDATE,
                       BH.FULF_CHANNEL,
                       BH.IBU_ID,
                       BH.IP_DATE,
                       BH.ITEM_QTY,
                       BH.ORDER_PRIORITY,
                       BH.PRIMARY_FC,
                       BH.QTY_REQD,
                       BH.RETAILER_NAME,
                       BH.RPT_DOMS_STATUS,
                       BH.SHIP_TO_COUNTRY,
                       BH.SHIP_TO_STATE,
                       BH.SUB_CHANNEL,
                       BH.SYSTEM_QTY,
                       BH.WORKCENTER,
                       'ACTIVE',
                       --Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3) begin
                       CASE
                          WHEN NVL (SO.MUST_ARRIVE_BY_DATE,
                                    TO_DATE ('1/1/0001', 'DD/MM/YYYY')) <
                                  TO_DATE ('1/1/2000', 'DD/MM/YYYY')
                          THEN
                             'N'
                          -- WHEN TO_DATE ('1/1/1900', 'DD/MM/YYYY') THEN 'N'
                          ELSE
                             'Y'
                       END
                          MABD_FLAG,
                       'N',
                       NULL,
                       NULL,
                       NVL (VEL_PRECISON_FLAG, 'V'),
                       SO.ESD,
                       SO.EST_DELIVERY_DATE,
                       --Changes to include MABD, FDD,DP Enabled and Velocity or Precision Flag (PPB-3) end

                       CASE
                          WHEN     BH.REGION_CODE = 'EMEA'
                               AND OM_HD.LOCAL_ORDER_TYPE LIKE '%Preshipment'
                          THEN
                             'PRESHIPMENT'
                          ELSE
                             'UNK'
                       END
                          AS LOCAL_ORDER_TYPE,
                       OM_HD.SALESREP_NAME,
                       p_mod,
                       null,
                       null,
                       om_order_type, --<RK004>
                       rsid  --<RK004>
                  FROM TABLE (t_get_order_tab_opr) T_ORDER
                       INNER JOIN SCDH_FULFILLMENT.BACKLOG_HEADER BH
                          ON T_ORDER.COLUMN_VALUE = BH.ORDER_NUMBER
                       LEFT JOIN SCDH_FULFILLMENT.SALES_ORDER_OPR SO
                          ON     BH.ORDER_NUMBER = SO.SALES_ORDER_ID
                             AND BH.REGION_CODE = SO.REGION_CODE
                             AND BH.BUID = SO.BUID
                       LEFT JOIN SCDH_FULFILLMENT.PROD_ORDER_OPR PO
                          ON (    PO.PROD_ORDER_NUM = BH.PROD_ORDER_NUMBER
                              AND PO.REGION_CODE = BH.REGION_CODE)
                       LEFT JOIN SCDH_FULFILLMENT.PROD_ORDER_LINE_OPR POL
                          ON (    BH.PROD_ORDER_NUMBER = POL.PROD_ORDER_NUM
                              AND BH.REGION_CODE = POL.REGION_CODE
                              AND BH.PROD_ORDER_LINE = POL.LINE_NUM)
                       LEFT JOIN SCDH_FULFILLMENT.OMS_ORDHDR OM_HD
                          ON (    OM_HD.ORDER_NUM = BH.ORDER_NUMBER
                              AND OM_HD.BU_ID = BH.BUID
                              AND OM_HD.OMS_SOURCE IN (p_OMS_SOURCE1,
                                                       p_OMS_SOURCE2))
                       LEFT OUTER JOIN SCDH_FULFILLMENT.MFG_ORD_BKLG_VP BVP
                          ON     bh.order_number = BVP.ORD_NUM
                             AND bh.buid = BVP.BU_ID
                             AND TRUNC (bh.TIE_NUMBER) =
                                    TRUNC (BVP.ORD_TIE_NUM)
                             AND bh.region_code = BVP.region

                 WHERE     BH.REGION_CODE = p_region --AND ORDER_NUMBER = p_order_num
                       AND BH.RPT_DOMS_STATUS = 'OPR')
                    LOG ERRORS INTO SCDH_AUDIT.ERR_OM_BACKLOG_DETAILS
                           ('INSERT OPR HEADER_STG-ACTIVE')
                           REJECT LIMIT UNLIMITED;
         END IF;

         IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
               P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
               P_NUMBER_OF_ROWS_INSERTED   => lv_insert_rec,
               P_NUMBER_OF_ROWS_UPDATED    => NULL,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'INSERTION DONE FOR OPR OM BACKLOG HEADER STG',
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => VAR_ERR_CODE,
               P_ERROR_MSG                 => VAR_ERR_MSG);
         END IF;

         IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
               P_STEP_NAME                 => 'INSERT',
               P_JOB_AUDIT_MESSAGE         =>    'INSERTING FOR OPR RECORDS INTO OM BACKLOG DETAIL STG FOR - '
                                              || p_region
                                              || '_'
                                              || p_mod,
               P_NUMBER_OF_ROWS_SELECTED   => NULL,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => NULL,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_IS_BASE_TABLE             => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'Insertion started..',
               P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
               P_JOB_INSTANCE_ID           => lv_job_instance_id,
               P_JOB_NAME                  => lv_job_name,
               P_JOB_AUDIT_LEVEL_ID        => NULL,
               P_COMMAND_NAME              => 'SQL',
               P_COMMAND_TYPE              => 'INSERT',
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => VAR_ERR_CODE,
               P_ERROR_MSG                 => VAR_ERR_MSG);
         END IF;

         lv_insert_rec := 0;
         lv_selcted_rec := 0;
         lv_error_location := 7.2;

         -------------------------------------OPEN CURSOR FOR FETCHING DATA FOR POPULATION OF OM_BACKLOG_DETAIL----------------------------
         INSERT INTO scdh_reporting.OM_BACKLOG_DETAIL_STG (
                        PROD_ORDER_NUM,
                        PROD_ORDER_LINE,
                        ORDER_NUM,
                        BU_ID,
                        REGION_CODE,
                        TIE_NUMBER,
                        JOB_TIME,
                        SPAMS_FACILITY,
                        OVERPACK_FACILITY,
                        KITTING_FACILITY,
                        BOXING_FACILITY,
                        FAST_TRACK_CONFIG_ID,
                        ISSUE_CODE,
                        FGA_ID,
                        SKU_NUM,
                        POS_APOS,
                        TIE_GROUP,
                        MOD_PART_NUMBER,
                        PART_NUMBER,
                        CONSUMPTION_FACILITY,
                        CONSUMPTION_STATUS,
                        BACKLOG_FACILITY,
                        EMBEDDED_SS_FLAG,
                        PART_QTY,
                        QTY_STARTED,
                        released_dmd,
                        unreleased_dmd,
                        FUTURE_DMD,
                        RETURN_ON_ASN,
                        PART_CONSUMPTION_FACILITY,
                        BASE_TYPE,
                        PART_TYPE_CODE,
                        is_fga_sku,
                        box_code,
                        consumed_dmd,
                        dmd_loc_facility,
                        dms_flag,
                        dtl_seq_num,
                        ibu_id,
                        item_category,
                        net_dmd,
                        part_type_reason_code,
                        qty_extended,
                        qty_reqd,
                        summary_dmd,
                        sys_ent_state,
                        RUN_DATE,
                        SYS_SOURCE,
                        SYS_CREATION_DATE,
                        SYS_LAST_MODIFIED_DATE,
                        demand_supply_region_code,
                        MOD_NUM,
                        IL_ITEM_TYPE,
                        IL_RETURN_ON_ASN,
                        I_DESCRIPTION)
            (SELECT bd.PROD_ORDER_NUMBER,
                    bd.PROD_ORDER_LINE,
                    bd.ORDER_NUMBER,
                    bd.BUID,
                    bd.REGION_CODE,
                    bd.TIE_NUMBER,
                    'HOURLY',
                    po.SPAMS_FACILITY,
                    po.OVERPACK_FACILITY,
                    po.KITTING_FACILITY,
                    po.BOXING_FACILITY,
                    NULL,
                    bd.ISSUE_CODE,
                    NULL,
                    bd.SKU,
                    NULL,
                    NULL,
                    bd.MOD_PART_NUMBER,
                    bd.PART_NUMBER,
                    bd.CONSUMPTION_FACILITY,
                    'UNCONSUMED',
                    bd.CONSUMPTION_FACILITY,
                    'UNK',
                    bd.summary_dmd,
                    bd.QTY_STARTED,
                    bd.released_dmd,
                    bd.unreleased_dmd,
                    bd.FUTURE_DMD,
                    NULL,
                    bd.CONSUMPTION_FACILITY,
                    NULL,
                    bd.PART_TYPE_CODE,
                    bd.is_fga_sku,
                    bd.box_code,
                    bd.consumed_dmd,
                    bd.dmd_loc_facility,
                    bd.dms_flag,
                    bd.dtl_seq_num,
                    bd.ibu_id,
                    bd.item_category,
                    bd.net_dmd,
                    bd.part_type_reason_code,
                    bd.qty_extended,
                    bd.qty_reqd,
                    bd.summary_dmd,
                    'ACTIVE',
                    SYSDATE,
                    'FDL',
                    SYSDATE,
                    SYSDATE,
                    fn_demand_supply_reg (bd.consumption_facility)
                       demand_supply_region_code,
                    p_mod,
                    NULL,
                    NULL,
                    NULL
               FROM TABLE (t_get_order_tab_opr) T_ORDER
                    INNER JOIN scdh_reporting.OM_BACKLOG_HEADER_STG bh
                       ON T_ORDER.COLUMN_VALUE = BH.ORDER_NUM
                    JOIN scdh_fulfillment.backlog_detail bd
                       ON     bh.region_code = bd.region_code
                          AND bh.order_num = bd.order_number
                          AND bh.prod_order_num = bd.prod_order_numBer
                          AND bh.tie_number = bd.tie_number
                          AND bh.bu_id = bd.buid
                    LEFT JOIN scdh_fulfillment.prod_order_opr po
                       ON     po.prod_order_num = bd.prod_order_number
                          AND po.region_code = bd.region_code
              WHERE     bd.region_code = p_region
                    AND bh.region_code = p_region
                    AND bh.rpt_doms_status = 'OPR'
                    AND bh.mod_num = p_mod)
                 LOG ERRORS INTO SCDH_AUDIT.ERR_OM_BACKLOG_DETAILS
                        ('INSERT OPR DETAIL_STG-ACTIVE')
                        REJECT LIMIT UNLIMITED;

         lv_insert_rec := SQL%ROWCOUNT;
         lv_selcted_rec := SQL%ROWCOUNT;

         ---updates the status of previous step in PROCESS_JOB_DETAIL TABLE.
         IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
               P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
               P_NUMBER_OF_ROWS_INSERTED   => lv_insert_rec,
               P_NUMBER_OF_ROWS_UPDATED    => NULL,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           => 'INSERTION DONE FOR OPR OM BACKLOG DETAIL STG',
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => VAR_ERR_CODE,
               P_ERROR_MSG                 => VAR_ERR_MSG);
         END IF;

         COMMIT;
      END LOOP;

      IF cur_get_order_OPR%ISOPEN
      THEN
         CLOSE cur_get_order_OPR;
      ELSIF cur_get_order_OPR_apj%ISOPEN
      THEN
         CLOSE cur_get_order_OPR_apj;
      ELSIF cur_get_order_opr_init%ISOPEN
      THEN
         CLOSE cur_get_order_opr_init;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;

         IF cur_get_order_opr%ISOPEN
         THEN
            CLOSE cur_get_order_opr;
         END IF;

         IF cur_get_order_opr_apj%ISOPEN
         THEN
            CLOSE cur_get_order_opr_apj;
         END IF;

         IF cur_get_order_opr_init%ISOPEN
         THEN
            CLOSE cur_get_order_opr_init;
         END IF;

         IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
               P_NUMBER_OF_ROWS_SELECTED   => NULL,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => NULL,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           =>    'ERROR AT LOCATION NUMBER '
                                              || lv_error_location,
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => VAR_ERR_CODE,
               P_ERROR_MSG                 => VAR_ERR_MSG);
         END IF;
   END prc_populating_header_dtl_opr;

   PROCEDURE prc_populate_om_summary (
      p_region            IN     SCDH_REPORTING.OM_BACKLOG_HEADER.REGION_CODE%TYPE,
      p_job_time          IN     SCDH_REPORTING.OM_BACKLOG_HEADER.job_time%TYPE,
      p_audit_log_yn      IN     CHAR,
      p_JOB_INSTANCE_ID          fdl_snop_scdhub.process_job_header.job_instance_id%TYPE,
      p_job_name          IN     VARCHAR2,
      p_err_code             OUT VARCHAR2,
      p_err_mesg             OUT VARCHAR2)
   IS
      ---------------------Local Variable Declaration----------------------------
      v_table_name          VARCHAR2 (100);
      v_partition_name      VARCHAR2 (100);
      VAR_ERR_CODE          VARCHAR2 (4000) := 0;
      VAR_ERR_MSG           VARCHAR2 (4000);
      v_query               VARCHAR2 (20000);
      lv_error_code         VARCHAR2 (4000);
      lv_error_msg          VARCHAR2 (4000);
      lv_error_location     NUMBER := 0;
      lv_tot_rec_cnt        NUMBER := 0;
      lv_selcted_rec        NUMBER := 0;
      lv_insert_rec         NUMBER := 0;
      lv_updated_rec        NUMBER := 0;
      lv_deletd_rec         NUMBER := 0;
      lv_indx               NUMBER := 0;
      lv_indx_1             NUMBER := 0;
      lv_cnt                NUMBER := 0;
      lv_tot_shipped_qty    NUMBER := 0;
      lv_max_ship_by_date   DATE;
      lv_result             BOOLEAN;
      exp_post              EXCEPTION;
      lv_job_audit_log_id   fdl_snop_scdhub.process_job_detail.job_audit_log_id%TYPE;
      lv_sql_1hr            VARCHAR2 (32767);
      lv_clob               CLOB;
   BEGIN
      lv_audit_log_yn := p_audit_log_yn;
      lv_job_instance_id := p_job_instance_id;
      lv_job_name := p_job_name;

      lv_error_location := 7.3;

      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_INSERT_JOB_DETAIL (
            P_STEP_NAME                 => 'CALCUALTE SUMMARY',
            P_JOB_AUDIT_MESSAGE         =>    'CALCULATION START FOR SUMMARY TABLE FOR REGION- '
                                           || p_region,
            P_NUMBER_OF_ROWS_SELECTED   => NULL,
            P_NUMBER_OF_ROWS_INSERTED   => NULL,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_IS_BASE_TABLE             => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'CALCULATION started..',
            P_DATABASE_PROCESS_ID       => USERENV ('SESSIONID'),
            P_JOB_INSTANCE_ID           => lv_job_instance_id,
            P_JOB_NAME                  => lv_job_name,
            P_JOB_AUDIT_LEVEL_ID        => NULL,
            P_COMMAND_NAME              => 'CALCUALTE',
            P_COMMAND_TYPE              => 'SQL',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => lv_error_code,
            P_ERROR_MSG                 => lv_error_msg);

         IF lv_error_code <> 0
         THEN
            p_err_code := lv_error_code;
            p_err_mesg :=
               lv_error_msg || 'Error Location:' || lv_error_location;
            RAISE exp_post;
         END IF;
      END IF;

      --DBMS_OUTPUT.PUT_LINE('BEFORE TRUNCATE');
      v_table_name := 'OM_BACKLOG_SUMMARY_' || p_region || '_stg';
      v_partition_name := 'JOB_TIME_' || p_region || '_H';
      SCDH_REPORTING.PPB2_UTILITY_pkg.TRUNCATE_TABLE (v_table_name,
                                                      VAR_err_code,
                                                      VAR_ERR_MSG);

      --DBMS_OUTPUT.PUT_LINE('AFTER TRUNCATE');
      IF VAR_err_code <> 0
      THEN
         p_err_code := VAR_err_code;
         p_err_mesg := VAR_ERR_MSG || 'Error Location:' || lv_error_location;
         RAISE exp_post;
      END IF;

      lv_error_location := 7.4;
      lv_sql_1hr :=
            'INSERT
                    INTO scdh_reporting.OM_BACKLOG_SUMMARY_'
         || p_region
         || '_stg(
                            order_source,
                            workcenter,
                            backlog_order_tie_type,
                            qty_type,
                            part_number,
                            demand_type,
                            facility,
                            site_name,
                            issue_code,
                            qty,
                            --pur_ccn,
                            ccn,
                            unconsumed_dmd,
                            consumed_dmd,
                            region_code,
                            fulfillment_region_code,
                            job_time,
                            sys_source,
                            sys_creation_date,
                            sys_last_modified_date,
                            update_date)
                     select order_source,
                            workcenter,
                            backlog_order_tie_type,
                            QTY_TYPE,
                            part_number,
                            DEMAND_TYPE,
                            facility_code,
                            site_name,
                            issue_code,
                            qty,
                            ccn,
                            unconsumed_DMD,
                            consumed_DMD,
                            region_code,
                            fulfillment_region_code,
                            :p_job_time,
                            ''SCDH'',
                            SYSTIMESTAMP,
                            SYSTIMESTAMP,
                            SYSTIMESTAMP
             from (
             WITH hd_dtl_data
              AS (SELECT bd.part_number,
                         bd.backlog_facility,
                         smf.site_name,
                         bd.issue_code,
                         --bh.pur_ccn,
                         bh.ccn,
                         bh.region_code,
                         bh.fulfillment_region_code,
                         nvl(bh.work_center,bh.workcenter) workcenter,
                         bd.released_dmd,
                         bd.consumption_status,
                         bd.part_qty,
                         bh.backlog_order_tie_type,
                         bd.unreleased_dmd,
                         bd.future_dmd,
                         bd.net_dmd,
                         bd.consumed_dmd,
                         bd.summary_dmd,
                         bh.rpt_doms_status,
                         DECODE (bh.workcenter, NULL, ''Z'', ''S'') AS qty_type,
                         bh.order_source
                    FROM scdh_reporting.OM_BACKLOG_HEADER bh
                         INNER JOIN
                         scdh_reporting.OM_BACKLOG_DETAIL bd
                            ON (    bh.region_code = bd.region_code
                                AND bh.bu_id = bd.bu_id
                                AND bh.order_num = bd.order_num
                                AND bh.prod_order_num = bd.prod_order_num
                                AND bh.tie_number = bd.tie_number
                                )
                         LEFT OUTER JOIN
                         scdh_master.facility smf
                            ON (    smf.facility_code = bd.backlog_facility
                                AND smf.sys_ent_state = ''ACTIVE'')
                    where     bd.issue_code in (''06'',''09'')
                        AND bh.region_code = :p_region
                                AND BH.job_time = :p_job_time
                                AND BD.job_time = :p_job_time
                                AND bd.sys_ent_state = ''ACTIVE''
                                AND bh.sys_ent_state = ''ACTIVE''
                                and bd.is_supressed=''N''
                                AND bd.consumption_status = ''UNCONSUMED''
                 )
           SELECT hd.part_number AS part_number,
                  hd.backlog_facility AS facility_code,
                  hd.site_name AS site_name,
                  hd.issue_code AS issue_code,
                  --hd.pur_ccn AS pur_ccn,
                  hd.ccn AS ccn,
                  hd.region_code AS region_code,
                  hd.fulfillment_region_code AS fulfillment_region_code,
                  ''R'' AS demand_type,
                  hd.workcenter AS workcenter,
                  qty_type,
                  hd.backlog_order_tie_type,
                  hd.order_source,
                  SUM (hd.summary_dmd) AS qty,
                  SUM (hd.net_dmd+hd.consumed_dmd) unconsumed_dmd,
                  0
                     AS consumed_dmd
             FROM hd_dtl_data hd
           WHERE hd.future_dmd=0
            and hd.rpT_DOMS_STATUS NOT IN (''IP'',''OPR'',''PP'',''HL'')
                 GROUP BY hd.region_code,
                  hd.fulfillment_region_code,
                 -- hd.pur_ccn,
                  hd.ccn,
                  hd.part_number,
                  hd.backlog_facility,
                  hd.site_name,
                  hd.qty_type,
                  hd.issue_code,
                  hd.backlog_order_tie_type,
                  workcenter,
                  order_source
         UNION ALL
           SELECT hd.part_number AS part_number,
                 hd.backlog_facility AS facility_code,
                  hd.site_name AS site_name,
                  hd.issue_code AS issue_code,
                  --hd.pur_ccn AS pur_ccn,
                  hd.ccn AS ccn,
                  hd.region_code AS region_code,
                  hd.fulfillment_region_code AS fulfillment_region_code,
                  DECODE (hd.rpt_doms_status,  ''PP'', ''W'',''OPR'', ''H'',''HL'',''H'',''U'')
                     AS demand_type,
                  hd.workcenter AS workcenter,
                  qty_type,
                  hd.backlog_order_tie_type,
                  hd.order_source,
                  SUM (hd.summary_dmd) AS qty,
                  SUM (hd.net_dmd+hd.consumed_dmd)
                     AS unconsumed_dmd,
                  0 consumed_dmd
             FROM hd_dtl_data hd
            WHERE hd.future_dmd=0
                    and hd.RpT_DOMS_STATUS  IN (''IP'',''OPR'',''PP'',''HL'')
         GROUP BY hd.region_code,
                  hd.fulfillment_region_code,
                 -- hd.pur_ccn,
                  hd.ccn,
                  hd.part_number,
                  hd.backlog_facility,
                  hd.site_name,
                  qty_type,
                  hd.issue_code,
                  hd.backlog_order_tie_type,
                  workcenter,
                  rpt_doms_status,
                  order_source
         UNION ALL
           SELECT hd.part_number AS part_number,
                  hd.backlog_facility AS facility_code,
                  hd.site_name AS site_name,
                  hd.issue_code AS issue_code,
                 -- hd.pur_ccn AS pur_ccn,
                  hd.ccn AS ccn,
                  hd.region_code AS region_code,
                  hd.fulfillment_region_code AS fulfillment_region_code,
                  DECODE (hd.rpt_doms_status,  ''OPR'', ''H'',''F'')
                     AS demand_type,
                  hd.workcenter AS workcenter,
                  qty_type,
                  hd.backlog_order_tie_type,
                  hd.order_source,
                  SUM (hd.summary_dmd) AS qty,
                  SUM (hd.net_dmd+hd.consumed_dmd)
                     AS unconsumed_dmd,
                  0 consumed_dmd
             FROM hd_dtl_data hd
            WHERE hd.future_dmd > 0
         GROUP BY hd.region_code,
                  hd.fulfillment_region_code,
                 -- hd.pur_ccn,
                  hd.ccn,
                  hd.part_number,
                  hd.backlog_facility,
                  hd.site_name,
                  qty_type,
                  hd.issue_code,
                  hd.backlog_order_tie_type,
                  workcenter,
                  rpt_doms_status,
                  order_source
                )
                  LOG ERRORS INTO SCDH_AUDIT.ERR_OM_BACKLOG_DETAILS(''INSERT SUMMARY 0609'') REJECT LIMIT UNLIMITED';

      lv_clob := lv_sql_1hr;

      EXECUTE IMMEDIATE lv_sql_1hr
         USING p_job_time,
               p_region,
               p_job_time,
               p_job_time;

      lv_error_location := 7.5;
      COMMIT;                                                               --

      EXECUTE IMMEDIATE
            'ALTER TABLE scdh_reporting.om_backlog_SUMMARY_0609 EXCHANGE subPARTITION '
         || v_partition_name
         || ' WITH TABLE SCDH_REPORTING.'
         || v_table_name
         || ' INCLUDING INDEXES WITH VALIDATION UPDATE INDEXES';

      IF lv_audit_log_yn = 'Y'
      THEN
         FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
            P_NUMBER_OF_ROWS_SELECTED   => lv_selcted_rec,
            P_NUMBER_OF_ROWS_INSERTED   => lv_insert_rec,
            P_NUMBER_OF_ROWS_UPDATED    => NULL,
            P_NUMBER_OF_ROWS_DELETED    => NULL,
            P_RUN_STATUS_CODE           => 0,
            P_RUN_STATUS_DESC           => 'SUMMARY CALCULATION IS DONE',
            P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
            P_ERROR_CODE                => VAR_ERR_CODE,
            P_ERROR_MSG                 => VAR_ERR_MSG);
      END IF;

      p_err_code := C_SUCCESS_CODE;
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         p_err_code := C_ERROR_CODE;
         p_err_mesg :=
               'Error in populate_om_summary Proc '
            || SUBSTR (SQLERRM, 1, 3500)
            || 'ERR LOC '
            || lv_error_location;

         IF lv_audit_log_yn = 'Y'
         THEN
            FDL_SNOP_SCDHUB.PROCESS_AUDIT_PKG.P_UPDATE_JOB_DETAIL (
               P_NUMBER_OF_ROWS_SELECTED   => NULL,
               P_NUMBER_OF_ROWS_INSERTED   => NULL,
               P_NUMBER_OF_ROWS_UPDATED    => NULL,
               P_NUMBER_OF_ROWS_DELETED    => NULL,
               P_RUN_STATUS_CODE           => 0,
               P_RUN_STATUS_DESC           =>    'SUMMARY CALCULATION FAILED AT ERR LOC '
                                              || lv_error_location,
               P_JOB_AUDIT_LOG_ID          => c_job_audit_log_id,
               P_ERROR_CODE                => VAR_ERR_CODE,
               P_ERROR_MSG                 => VAR_ERR_MSG);
         END IF;
   END prc_populate_om_summary;

   FUNCTION GET_TXN_MAX_SIZE_ROW_LIMIT (I_OWNER_NAME    VARCHAR2,
                                        I_TABLE_NAME    VARCHAR2)
      RETURN NUMBER
   AS
      V_MAX_TXN_SIZE   NUMBER;
      V_MAX_TXN_ROWS   NUMBER;
   BEGIN
      SELECT PROPVALUE
        INTO V_MAX_TXN_SIZE
        FROM FDL_SNOP_SCDHUB.MST_SYS_PROPS
       WHERE ID = 'MAX_TXN_HIGH_SIZE';

      IF V_MAX_TXN_SIZE IS NULL
      THEN
         RAISE NO_DATA_FOUND;
      END IF;

      SELECT FLOOR ( (V_MAX_TXN_SIZE * 1024 * 1024) / AVG_ROW_LEN)
        INTO V_MAX_TXN_ROWS
        FROM ALL_TABLES
       WHERE OWnER = I_OWNER_NAME AND TABLE_NAME = I_TABLE_NAME;

      IF V_MAX_TXN_ROWS IS NULL
      THEN
         RAISE NO_DATA_FOUND;
      END IF;

      RETURN V_MAX_TXN_ROWS;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN 10000;
   END GET_TXN_MAX_SIZE_ROW_LIMIT;
END om_piece_part_bcklg_pkg;