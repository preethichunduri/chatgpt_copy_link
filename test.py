from core.utilities.logging import *
from core.database import db, Location, Condition, NSTrackingLevel1, NSTrackingLevel2, Action
from core.audit_constants import AuditOps, AuditEvents
from core.constants import TREND_UP, TREND_DOWN, TREND_FLAT, INSIGHT_ICON_HIGH_ENGAGEMENT_CAMPAIGN, \
    INSIGHT_ICON_LOW_ENGAGEMENT_CAMPAIGN, INSIGHT_ICON_HIGH_ENGAGEMENT_NUDGE, INSIGHT_ICON_LOW_ENGAGEMENT_NUDGE, \
    INSIGHT_ICON_NUDGE_TODAY, TREND_NO_SUFFICIENT_DATA, INSIGHT_STATIC_CARD_NUDGE_TODAY_TITLE, CONDITION_TYPES, SENT, \
    INPROGRESS


def messaging_reach_query(org_id,interval_high,interval_low,nudge_today=None,message_id=None,nudge_type=None,is_campaign=None):
    try:
        sql_query_params = {}
        FUNCTION_NAME = "messaging_reach_query"
        sql_query = f"WITH permissionrate AS ( " \
                    f"SELECT   udp.organization_id ," \
                    f"Least(.9,(Sum(" \
                    f"CASE WHEN udp.notification_permission = 'Accept' THEN 1 " \
                    f"ELSE 0 END) / Count(udp.id))) permissionrate " \
                    f"FROM     userdata_permissions udp WHERE    udp.notification_permission IS NOT NULL " \
                    f"AND      udp.organization_id = :org_id " \
                    f"AND      Date(udp.date) BETWEEN Curdate() - interval :interval_high_pr day AND Curdate() - interval :interval_low_pr day " \
                    f"GROUP BY udp.organization_id) "


        if message_id is None and nudge_type is None and is_campaign is None:
            sql_query += f", nudgesreceived AS (SELECT     nr.message_id messageid ," \
                    f"COALESCE(round(count(nr.id) * pr.permissionrate,0),0) receivedcount FROM nudges_sent ns " \
                    f"INNER JOIN nudges_received nr ON         ns.id = nr.nudge_sent_id AND nr.nudge_sent_id_code IN (1,4) AND  nr.organization_id = :org_id " \
                    f"INNER JOIN token t ON  nr.device_id = t.device_id AND t.active = 1 and t.platform = 'ios' and t.organization_id = :org_id " \
                    f"JOIN   permissionrate pr ON         pr.organization_id = ns.organization_id " \
                    f"AND    date(nr.timestamp) between curdate() - interval :interval_high day AND curdate() - interval :interval_low day " \
                    f"GROUP BY   nr.message_id), nudgestapped AS (SELECT     nt.message_id messageid ,COALESCE(count(nt.id),0)  tappedcount " \
                    f"FROM       nudges_sent ns INNER JOIN nudges_tapped nt ON ns.id = nt.nudge_sent_id AND nt.nudge_sent_id_code IN (1,4) " \
                    f"INNER JOIN token t ON nt.device_id = t.device_id AND t.active = 1 and t.platform = 'ios' and t.organization_id = :org_id " \
                    f"WHERE      nt.organization_id = :org_id " \
                    f"AND        date(nt.timestamp) between curdate() - interval :interval_high day AND curdate() - interval :interval_low day " \
                    f"GROUP BY   nt.message_id ), messagelist AS (SELECT     a.id                                       actionid, " \
                    f"m.id messageid ,m.NAME  nudgename ," \
                    f"m.organization_id  orgid,COALESCE(c.category_value,'Uncategorized') nudgecategory," \
                    f"ifnull(c.sort_order,9999)  categorysortorder,min(ns.timestamp) firstsent," \
                    f"round(count(distinct t.token) * pr.permissionrate,0) sentcountpr,round(count(t.token) * pr.permissionrate,0) sentcount,ns.nudge_type " \
                    f"FROM       message m INNER JOIN action a ON         a.message_id = m.id and a.action_type_id = 1 " \
                    f"INNER JOIN nudges_sent ns ON         m.id = ns.message_id JOIN permissionrate pr " \
                    f"ON pr.organization_id = ns.organization_id " \
                    f"inner join token t on ns.device_id = t.device_id and t.active = 1 and t.organization_id = :org_id " \
                    f"inner join track_nudges_sent tns on tns.nudge_sent_id = ns.id and tns.nudge_sent_flag = 1 LEFT JOIN  categories c " \
                    f"ON         m.message_category_id = c.category_key WHERE m.organization_id = :org_id and c.category_value != 'Test nudge' " \
                    f"AND        date(ns.timestamp) between curdate() - interval :interval_high day AND curdate() - interval :interval_low day " \
                    f"GROUP BY   1,2,3,4) ,members AS (SELECT round(count(distinct t.token) * pr.permissionrate,0) " \
                    f"AS token_count,ns.organization_id orgid FROM       nudges_sent ns JOIN       permissionrate pr ON         pr.organization_id = ns.organization_id " \
                    f"INNER JOIN token t ON ns.device_id = t.device_id AND t.active = 1 and t.organization_id = :org_id " \
                    f"inner JOIN track_nudges_sent tns on ns.id = tns.nudge_sent_id and tns.nudge_sent_flag = 1 " \
                    f"WHERE date(ns.timestamp) between curdate() - interval :interval_high day AND curdate() - interval :interval_low day AND  ns.organization_id = :org_id) " \
                    f"select COALESCE(mb.token_count,0) unique_members, " \
                    f"COALESCE(sum(ml.sentcount),0)   AS totalimpressions , " \
                    f"COALESCE(sum(nr.receivedcount),0) as totalreceived ," \
                    f"COALESCE(sum(nt.tappedcount),0) as totaltapped," \
                    f"round(COALESCE((sum(nt.tappedcount)/sum(nr.receivedcount)),0) * 100,1) as engagementrate " \
                    f",COALESCE(round(sum(ml.sentcount) * (sum(nt.tappedcount)/sum(nr.receivedcount)),0),0) as totalengagements " \
                    f"from members mb join messagelist ml on mb.orgid = ml.orgid " \
                    f"left join nudgesreceived nr on ml.messageid = nr.messageid left join nudgestapped nt on ml.messageid = nt.messageid"


        if message_id and nudge_type != 8 and is_campaign is None:
            sql_query += f", nudgesreceived AS (" \
                         f"SELECT     nr.message_id                                         messageid ," \
                         f"COALESCE(Round(Count(nr.id) * pr.permissionrate,0),0) receivedcount " \
                         f"FROM nudges_received nr INNER JOIN nudges_sent ns ON  ns.id = nr.nudge_sent_id " \
                         f"AND  nr.nudge_sent_id_code IN (1,4) AND nr.organization_id = :org_id " \
                         f"AND nr.message_id = :message_id INNER JOIN token t ON nr.device_id = t.device_id " \
                         f"AND t.active = 1 AND t.platform = 'ios' AND t.organization_id = :org_id JOIN permissionrate pr " \
                         f"ON pr.organization_id = ns.organization_id " \
                         f"AND  Date(nr.timestamp) BETWEEN Curdate() - interval :interval_high day AND Curdate() - interval :interval_low day " \
                         f"WHERE      nr.message_id = :message_id GROUP BY   nr.message_id), nudgestapped AS (" \
                         f"SELECT     nt.message_id            messageid , COALESCE(Count(nt.id),0) tappedcount " \
                         f"FROM       nudges_tapped nt INNER JOIN nudges_sent ns ON ns.id = nt.nudge_sent_id " \
                         f"AND        nt.nudge_sent_id_code IN (1,4) AND nt.message_id = :message_id INNER JOIN token t " \
                         f"ON         nt.device_id = t.device_id AND t.active = 1 " \
                         f"AND t.platform = 'ios' AND t.organization_id = :org_id WHERE  nt.organization_id = :org_id " \
                         f"AND nt.message_id = :message_id AND Date(nt.timestamp) BETWEEN Curdate() - interval :interval_high day " \
                         f"AND Curdate() - interval :interval_low day GROUP BY   nt.message_id ), messagelist AS " \
                         f"(SELECT     a.id                                                 actionid, " \
                         f"m.id                                                 messageid , " \
                         f"m.NAME                                               nudgename , " \
                         f"m.organization_id                                    orgid, " \
                         f"COALESCE(c.category_value,'Uncategorized')           nudgecategory, " \
                         f"Ifnull(c.sort_order,9999)                            categorysortorder, " \
                         f"Min(ns.timestamp)                                    firstsent, " \
                         f"Round(Count(DISTINCT t.token) * pr.permissionrate,0) sentcountpr, " \
                         f"round(count(t.token) * pr.permissionrate,0) sentcount,ns.nudge_type " \
                         f"FROM       message m INNER JOIN action a ON         a.message_id = m.id " \
                         f"AND        a.action_type_id = 1 AND        m.id = :message_id INNER JOIN nudges_sent ns " \
                         f"ON         m.id = ns.message_id AND        ns.message_id=:message_id JOIN       permissionrate pr " \
                         f"ON         pr.organization_id = ns.organization_id INNER JOIN token t ON         ns.device_id = t.device_id " \
                         f"AND        t.active = 1 AND        t.organization_id = :org_id INNER JOIN track_nudges_sent tns " \
                         f"ON         ns.id = tns.nudge_sent_id AND        tns.nudge_sent_flag = 1 AND        tns.message_id = :message_id " \
                         f"LEFT JOIN  categories c ON m.message_category_id = c.category_key WHERE m.id = :message_id AND        m.organization_id = :org_id " \
                         f"AND        Date(ns.timestamp) BETWEEN Curdate() - interval :interval_high day " \
                         f"AND        Curdate() - interval :interval_low day GROUP BY   1,2,3,4) ,members AS ( " \
                         f"SELECT     Round(Count(DISTINCT t.token) * pr.permissionrate,0) AS token_count, " \
                         f"ns.organization_id orgid FROM       nudges_sent ns " \
                         f"JOIN       permissionrate pr ON  pr.organization_id = ns.organization_id " \
                         f"INNER JOIN token t ON         ns.device_id = t.device_id " \
                         f"AND        t.active = 1 AND        t.organization_id = :org_id " \
                         f"INNER JOIN track_nudges_sent tns " \
                         f"ON         ns.id = tns.nudge_sent_id AND tns.nudge_sent_flag = 1 AND tns.message_id = :message_id " \
                         f"WHERE      Date(ns.timestamp) BETWEEN Curdate() - interval :interval_high day AND Curdate() - interval :interval_low day " \
                         f"AND        ns.organization_id = :org_id) SELECT COALESCE(mb.token_count,0) unique_members," \
                         f"COALESCE(Sum(ml.sentcount),0)                                                        AS totalimpressions ," \
                         f"COALESCE(Sum(nr.receivedcount),0)                                                      AS totalreceived ," \
                         f"COALESCE(Sum(nt.tappedcount),0)                                                        AS totaltapped," \
                         f"Round(COALESCE((Sum(nt.tappedcount)/Sum(nr.receivedcount)),0) * 100,1)                 AS engagementrate ," \
                         f"COALESCE(Round(Sum(ml.sentcount) * (Sum(nt.tappedcount)/Sum(nr.receivedcount)),0),0) AS totalengagements " \
                         f"FROM      members mb JOIN      messagelist ml ON        mb.orgid = ml.orgid LEFT JOIN nudgesreceived nr " \
                         f"ON        ml.messageid = nr.messageid LEFT JOIN nudgestapped nt ON  ml.messageid = nt.messageid "

        # Time based nudges
        if message_id and nudge_type == 8 and is_campaign is None:
            sql_query += f", nudgesreceived AS (" \
                         f"SELECT     nr.message_id                                         messageid ," \
                         f"COALESCE(Round(Count(nr.id) * pr.permissionrate,0),0) receivedcount " \
                         f"FROM nudges_received nr INNER JOIN nudges_sent ns ON  ns.id = nr.nudge_sent_id " \
                         f"AND  nr.nudge_sent_id_code IN (1,4) AND nr.organization_id = :org_id " \
                         f"AND nr.message_id = :message_id INNER JOIN token t ON nr.device_id = t.device_id " \
                         f"AND t.active = 1 AND t.platform = 'ios' AND t.organization_id = :org_id JOIN permissionrate pr " \
                         f"ON pr.organization_id = ns.organization_id " \
                         f"WHERE      nr.message_id = :message_id GROUP BY   nr.message_id), nudgestapped AS (" \
                         f"SELECT     nt.message_id            messageid , COALESCE(Count(nt.id),0) tappedcount " \
                         f"FROM       nudges_tapped nt INNER JOIN nudges_sent ns ON ns.id = nt.nudge_sent_id " \
                         f"AND        nt.nudge_sent_id_code IN (1,4) AND nt.message_id = :message_id INNER JOIN token t " \
                         f"ON         nt.device_id = t.device_id AND t.active = 1 " \
                         f"AND t.platform = 'ios' AND t.organization_id = :org_id WHERE  nt.organization_id = :org_id " \
                         f"GROUP BY   nt.message_id ), messagelist AS " \
                         f"(SELECT     a.id                                                 actionid, " \
                         f"m.id                                                 messageid , " \
                         f"m.NAME                                               nudgename , " \
                         f"m.organization_id                                    orgid, " \
                         f"COALESCE(c.category_value,'Uncategorized')           nudgecategory, " \
                         f"Ifnull(c.sort_order,9999)                            categorysortorder, " \
                         f"Min(ns.timestamp)                                    firstsent, " \
                         f"Round(Count(DISTINCT t.token) * pr.permissionrate,0) sentcountpr, " \
                         f"round(count(t.token) * pr.permissionrate,0) sentcount,ns.nudge_type " \
                         f"FROM       message m INNER JOIN action a ON         a.message_id = m.id " \
                         f"AND        a.action_type_id = 1 AND        m.id = :message_id INNER JOIN nudges_sent ns " \
                         f"ON         m.id = ns.message_id AND        ns.message_id=:message_id JOIN       permissionrate pr " \
                         f"ON         pr.organization_id = ns.organization_id INNER JOIN token t ON         ns.device_id = t.device_id " \
                         f"AND        t.active = 1 AND        t.organization_id = :org_id INNER JOIN track_nudges_sent tns " \
                         f"ON         ns.id = tns.nudge_sent_id AND        tns.nudge_sent_flag = 1 AND        tns.message_id = :message_id " \
                         f"LEFT JOIN  categories c ON m.message_category_id = c.category_key WHERE m.id = :message_id AND        m.organization_id = :org_id " \
                         f"GROUP BY   1,2,3,4) ,members AS ( " \
                         f"SELECT     Round(Count(DISTINCT t.token) * pr.permissionrate,0) AS token_count, " \
                         f"ns.organization_id orgid FROM       nudges_sent ns " \
                         f"JOIN       permissionrate pr ON  pr.organization_id = ns.organization_id " \
                         f"INNER JOIN token t ON         ns.device_id = t.device_id " \
                         f"AND        t.active = 1 AND        t.organization_id = :org_id " \
                         f"INNER JOIN track_nudges_sent tns " \
                         f"ON         ns.id = tns.nudge_sent_id AND tns.nudge_sent_flag = 1 AND tns.message_id = :message_id " \
                         f"WHERE  ns.organization_id = :org_id) SELECT COALESCE(mb.token_count,0) unique_members," \
                         f"COALESCE(Sum(ml.sentcount),0)                                                        AS totalimpressions ," \
                         f"COALESCE(Sum(nr.receivedcount),0)                                                      AS totalreceived ," \
                         f"COALESCE(Sum(nt.tappedcount),0)                                                        AS totaltapped," \
                         f"Round(COALESCE((Sum(nt.tappedcount)/Sum(nr.receivedcount)),0) * 100,1)                 AS engagementrate ," \
                         f"COALESCE(Round(Sum(ml.sentcount) * (Sum(nt.tappedcount)/Sum(nr.receivedcount)),0),0) AS totalengagements " \
                         f"FROM      members mb JOIN      messagelist ml ON        mb.orgid = ml.orgid LEFT JOIN nudgesreceived nr " \
                         f"ON        ml.messageid = nr.messageid LEFT JOIN nudgestapped nt ON  ml.messageid = nt.messageid "

        if message_id and nudge_type == 1 and is_campaign == 1:
                sql_query += f",nudgesreceived AS (SELECT " \
                f"nr.message_id messageid,COALESCE(Round(Count(nr.id) * pr.permissionrate,0),0) receivedcount , " \
                f"cn.campaign_id campaign_id FROM nudges_received nr INNER JOIN nudges_sent ns ON ns.id = nr.nudge_sent_id " \
                f"AND nr.nudge_sent_id_code IN (1, 4) AND nr.organization_id = :org_id " \
                f"inner join action a on a.message_id = nr.message_id inner join campaign_nudges cn " \
                f"on cn.nudge_id = a.id and campaign_id = :campaign_id INNER JOIN token t ON nr.device_id = t.device_id " \
                f"AND t.active = 1 AND t.platform = 'ios' AND t.organization_id = :org_id " \
                f"JOIN permissionrate pr ON pr.organization_id = ns.organization_id AND Date(nr.timestamp) BETWEEN Curdate() - interval :interval_high day " \
                f"AND Curdate() - interval :interval_low day GROUP BY nr.message_id ),nudgestapped AS (SELECT nt.message_id messageid, " \
                f"COALESCE(Count(nt.id),0) tappedcount , cn.campaign_id campaign_id FROM nudges_tapped nt " \
                f"INNER JOIN nudges_sent ns ON ns.id = nt.nudge_sent_id AND nt.nudge_sent_id_code IN (1, 4) " \
                f"inner join action a on a.message_id = nt.message_id inner join campaign_nudges cn on cn.nudge_id = a.id and campaign_id = :campaign_id " \
                f"INNER JOIN token t ON nt.device_id = t.device_id AND t.active = 1 AND t.platform = 'ios' AND t.organization_id = :org_id " \
                f"WHERE nt.organization_id = :org_id AND Date(nt.timestamp) BETWEEN Curdate() - interval :interval_high day " \
                f"AND Curdate() - interval :interval_low day GROUP BY nt.message_id ), messagelist AS (SELECT a.id actionid, " \
                f"cn.campaign_id campaign_id,m.id messageid, m.NAME nudgename, m.organization_id orgid, " \
                f"COALESCE(c.category_value, 'Uncategorized' ) nudgecategory, Ifnull(c.sort_order, 9999) categorysortorder, " \
                f"Min(ns.timestamp) firstsent, Round(Count(DISTINCT t.token) * pr.permissionrate,0) sentcountpr," \
                f"Round(Count(t.token) * pr.permissionrate,0) sentcount,ns.nudge_type FROM message m INNER JOIN action a " \
                f"ON a.message_id = m.id AND a.action_type_id = 1 inner join campaign_nudges cn on cn.nudge_id = a.id " \
                f"and cn.campaign_id = :campaign_id INNER JOIN nudges_sent ns ON m.id = ns.message_id " \
                f"JOIN permissionrate pr ON pr.organization_id = ns.organization_id INNER JOIN token t " \
                f"ON ns.device_id = t.device_id AND t.active = 1 AND t.organization_id = :org_id " \
                f"INNER JOIN track_nudges_sent tns ON ns.id = tns.nudge_sent_id AND tns.nudge_sent_flag = 1 " \
                f"LEFT JOIN categories c ON m.message_category_id = c.category_key WHERE m.organization_id = :org_id " \
                f"AND Date(ns.timestamp) BETWEEN Curdate() - interval :interval_high day AND Curdate() - interval :interval_low day " \
                f"GROUP BY 1,2,3,4),members AS (SELECT Round(Count(DISTINCT t.token) * pr.permissionrate,0) AS token_count," \
                f"ns.organization_id orgid ,cn.campaign_id campaign_id FROM nudges_sent ns JOIN permissionrate pr ON pr.organization_id = ns.organization_id " \
                f"INNER JOIN token t ON ns.device_id = t.device_id AND t.active = 1 AND t.organization_id = :org_id " \
                f"INNER JOIN track_nudges_sent tns ON ns.id = tns.nudge_sent_id AND tns.nudge_sent_flag = 1 " \
                f"inner join action a on a.message_id = tns.message_id inner join campaign_nudges cn on cn.nudge_id = a.id " \
                f"and cn.campaign_id = :campaign_id WHERE Date(ns.timestamp) BETWEEN Curdate() - interval :interval_high day " \
                f"AND Curdate() - interval :interval_low day AND ns.organization_id = :org_id AND cn.campaign_id = :campaign_id ) " \
                f"SELECT COALESCE(mb.token_count, 0) unique_members,COALESCE(Sum(ml.sentcount),0) AS totalimpressions," \
                f"COALESCE(Sum(nr.receivedcount),0) AS totalreceived,COALESCE(Sum(nt.tappedcount),0) AS totaltapped," \
                f"Round(COALESCE((Sum(nt.tappedcount)/ Sum(nr.receivedcount)),0) * 100,1) AS engagementrate," \
                f"COALESCE(Round(Sum(ml.sentcountpr) * (Sum(nt.tappedcount)/ Sum(nr.receivedcount)),0),0) AS totalengagements FROM members mb " \
                f"JOIN messagelist ml ON mb.orgid = ml.orgid LEFT JOIN nudgesreceived nr ON ml.messageid = nr.messageid " \
                f"LEFT JOIN nudgestapped nt ON ml.messageid = nt.messageid"

        # geo nudge
        if message_id and nudge_type != 8 and is_campaign is None:
            sql_query_params["message_id"] = message_id
            sql_query_params["interval_high"] = interval_high
            sql_query_params["interval_low"] = interval_low
        if message_id and nudge_type == 1 and is_campaign == 1:
            sql_query_params["campaign_id"] = message_id
            sql_query_params["interval_high"] = interval_high
            sql_query_params["interval_low"] = interval_low
        if message_id and nudge_type == 8 and is_campaign is None:
            sql_query_params["message_id"] = message_id
        else:
            sql_query_params["interval_high"] = interval_high
            sql_query_params["interval_low"] = interval_low
        sql_query_params["org_id"] = org_id
        sql_query_params["interval_high_pr"] = CONSTANT_PERMISSION_RATE_INTERVAL_HIGH if nudge_today else 30 if nudge_type == 1 else interval_high
        sql_query_params["interval_low_pr"] = CONSTANT_PERMISSION_RATE_INTERVAL_LOW if nudge_today else 0 if nudge_type == 1 else interval_low
        res = db.session.execute(sql_query, sql_query_params)
        results = res.fetchall()
        res = [dict(r) for r in results]
        msg = f'message_id {message_id} with interval_high = {interval_high} and interval_low = {interval_low} nudge_today = {nudge_today} query = {sql_query}, sql_query_params = {sql_query_params}'
        log_msg(SEVERITY_INFO, FILE_NAME, FUNCTION_NAME, msg)
    except Exception as e:
        track_error(e, FILE_NAME, "message_reach_query")
        raise InternalServerError(str(e))
    return res


def calc_trend_count(new_unique_members, old_unique_members, new_totalimpressions, old_totalimpressions,
                     new_totalengagements,
                     old_totalengagements, new_engagement_rate, old_engagement_rate):
    try:
        """
            Function to calculate trends
            :return: trend value, and trend percent
        """

        members_messaged_percent_trend = 0
        total_impressions_percent_trend = 0
        engagements_percent_trend = 0
        engagements_rate_percent_trend = 0

        # Calculate percentage value for unique members
        if old_unique_members:
            members_messaged_percent_trend_number = (new_unique_members - old_unique_members) / old_unique_members
            members_messaged_percent_trend = members_messaged_percent_trend_number * 100

            # Define trends for unique members based on percent value
            if members_messaged_percent_trend > 0:
                members_messaged_trend_arrow = TREND_UP
            elif members_messaged_percent_trend < 0:
                members_messaged_trend_arrow = TREND_DOWN
            else:
                members_messaged_trend_arrow = TREND_FLAT
        else:
            members_messaged_trend_arrow = TREND_NO_SUFFICIENT_DATA

        # Calculate percentage value for total impressions
        if old_totalimpressions:
            total_impressions_percent_trend_number = (
                                                             new_totalimpressions - old_totalimpressions) / old_totalimpressions
            total_impressions_percent_trend = total_impressions_percent_trend_number * 100

            # Define trends for total impressions based on percent value
            if total_impressions_percent_trend > 0:
                total_impressions_trend_arrow = TREND_UP
            elif total_impressions_percent_trend < 0:
                total_impressions_trend_arrow = TREND_DOWN
            else:
                total_impressions_trend_arrow = TREND_FLAT
        else:
            total_impressions_trend_arrow = TREND_NO_SUFFICIENT_DATA

        # Calculate percentage value for total engagements
        if old_totalengagements:
            engagements_percent_trend_number = (new_totalengagements - old_totalengagements) / old_totalengagements
            engagements_percent_trend = engagements_percent_trend_number * 100

            # Define trends for total engagements based on percent value
            if engagements_percent_trend > 0:
                engagements_trend_arrow = TREND_UP
            elif engagements_percent_trend < 0:
                engagements_trend_arrow = TREND_DOWN
            else:
                engagements_trend_arrow = TREND_FLAT
        else:
            engagements_trend_arrow = TREND_NO_SUFFICIENT_DATA

        # Calculate percentage value for engagement rate
        if old_engagement_rate:
            engagements_rate_trend_number = (new_engagement_rate - old_engagement_rate) / old_engagement_rate
            engagements_rate_percent_trend = engagements_rate_trend_number * 100

            # Define trends for total engagements based on percent value
            if engagements_rate_percent_trend > 0:
                engagements_rate_trend_arrow = TREND_UP
            elif engagements_rate_percent_trend < 0:
                engagements_rate_trend_arrow = TREND_DOWN
            else:
                engagements_rate_trend_arrow = TREND_FLAT
        else:
            engagements_rate_trend_arrow = TREND_NO_SUFFICIENT_DATA

        return members_messaged_percent_trend, members_messaged_trend_arrow, total_impressions_percent_trend, \
            total_impressions_trend_arrow, engagements_percent_trend, engagements_trend_arrow, \
            engagements_rate_percent_trend, engagements_rate_trend_arrow

    except Exception as e:
        raise e


@celery_app.task(bind=True, acks_late=True)
def calc_messaging_reach_async_delay(self):
    """
        Function to calculate analytics messaging reach query asynchronously
        :return: 201
    """
    from core.views.analytics_dashboard import messaging_reach_query
    FUNCTION_NAME = "calc_messaging_reach_async_delay"
    RDS_MESSAGING_REACH_TABLE = "analytics_messaging_reach_by_org"
    RDS_CATEGORY_OVERVIEW_TABLE = "analytics_category_overview_by_org"
    RDS_INSIGHTS_TABLE = "analytics_insights_by_org"
    total_updated = 0
    total_inserted = 0
    total_errors = 0
    insert_update = None
    num_orgs = 0
    timer = Timer()
    timer.start()
    try:
        from core.app import create_app
        with create_app().app_context():
            with app.test_request_context('/analytics-dashboard/async_query'):
                try:
                    task_id = self.request.id
                    msg = f"TASK_ID:{task_id} Start calculating messaging reach"
                    log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)

                    sql_str = (f'select o.id as id from organization o inner join organization_type ot '
                               f'on o.organization_type_id = ot.id where o.active = 1 and ot.calculate_analytics = 1 '
                               f'order by id desc')
                    # Run the SQL
                    result = db.session.execute(sql_str)

                    for id in result:
                        try:
                            org_id = id[0]
                            msg = f"TASK_ID:{task_id} Start calculating messaging reach for org_id {org_id}"
                            log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)

                            res_180 = messaging_reach_query(org_id=org_id, interval_high=180, interval_low=91)[0]
                            res_90 = messaging_reach_query(org_id=org_id, interval_high=90, interval_low=1)[0]

                            # Trend and percent calculation
                            result_trends_90 = calc_trend_count(res_90["unique_members"], res_180["unique_members"],
                                                                res_90["totalimpressions"], res_180["totalimpressions"],
                                                                res_90["totalengagements"], res_180["totalengagements"],
                                                                res_90["engagementrate"], res_180["engagementrate"])

                            last_90day_members_messaged_percent_trend = result_trends_90[0]
                            last_90day_members_messaged_trend_arrow = result_trends_90[1]
                            last_90day_total_impressions_percent_trend = result_trends_90[2]
                            last_90day_total_impressions_trend_arrow = result_trends_90[3]
                            last_90day_engagements_percent_trend = result_trends_90[4]
                            last_90day_engagements_trend_arrow = result_trends_90[5]
                            last_90day_engagements_rate_percent_trend = result_trends_90[6]
                            last_90day_engagements_rate_trend_arrow = result_trends_90[7]

                            res_60 = messaging_reach_query(org_id=org_id, interval_high=60, interval_low=31)[0]
                            res_30 = messaging_reach_query(org_id=org_id, interval_high=30, interval_low=1)[0]

                            # Trend and percent calculation
                            result_trends_30 = calc_trend_count(res_30["unique_members"], res_60["unique_members"],
                                                                res_30["totalimpressions"], res_60["totalimpressions"],
                                                                res_30["totalengagements"], res_60["totalengagements"],
                                                                res_30["engagementrate"], res_60["engagementrate"])

                            last_month_members_messaged_percent_trend = result_trends_30[0]
                            last_month_members_messaged_trend_arrow = result_trends_30[1]
                            last_month_total_impressions_percent_trend = result_trends_30[2]
                            last_month_total_impressions_trend_arrow = result_trends_30[3]
                            last_month_engagements_percent_trend = result_trends_30[4]
                            last_month_engagements_trend_arrow = result_trends_30[5]
                            last_month_engagements_rate_percent_trend = result_trends_30[6]
                            last_month_engagements_rate_trend_arrow = result_trends_30[7]

                            res_14 = messaging_reach_query(org_id=org_id, interval_high=14, interval_low=8)[0]
                            res_7 = messaging_reach_query(org_id=org_id, interval_high=7, interval_low=1)[0]

                            # Trend and percent calculation
                            result_trends_7 = calc_trend_count(res_7["unique_members"], res_14["unique_members"],
                                                               res_7["totalimpressions"], res_14["totalimpressions"],
                                                               res_7["totalengagements"], res_14["totalengagements"],
                                                               res_7["engagementrate"], res_14["engagementrate"])

                            last_week_members_messaged_percent_trend = result_trends_7[0]
                            last_week_members_messaged_trend_arrow = result_trends_7[1]
                            last_week_total_impressions_percent_trend = result_trends_7[2]
                            last_week_total_impressions_trend_arrow = result_trends_7[3]
                            last_week_engagements_percent_trend = result_trends_7[4]
                            last_week_engagements_trend_arrow = result_trends_7[5]
                            last_week_engagements_rate_percent_trend = result_trends_7[6]
                            last_week_engagements_rate_trend_arrow = result_trends_7[7]

                            # try to update 1st
                            params = {"timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                                      "organization_id": org_id,
                                      "last_week_members_messaged": int(res_7["unique_members"]),
                                      "last_week_members_messaged_percent_trend": int(
                                          last_week_members_messaged_percent_trend),
                                      "last_week_members_messaged_trend_arrow": last_week_members_messaged_trend_arrow,
                                      "last_week_total_impressions": int(res_7["totalimpressions"]),
                                      "last_week_total_impressions_percent_trend": int(
                                          last_week_total_impressions_percent_trend),
                                      "last_week_total_impressions_trend_arrow": last_week_total_impressions_trend_arrow,
                                      "last_week_engagements": int(res_7["totalengagements"]),
                                      "last_week_engagements_percent_trend": int(last_week_engagements_percent_trend),
                                      "last_week_engagements_trend_arrow": last_week_engagements_trend_arrow,
                                      "last_week_engagements_rate_trend": round(res_7["engagementrate"], 1),
                                      "last_week_engagements_rate_percent_trend": round(
                                          last_week_engagements_rate_percent_trend, 1),
                                      "last_week_engagements_rate_trend_arrow": last_week_engagements_rate_trend_arrow,
                                      "last_month_members_messaged": int(res_30["unique_members"]),
                                      "last_month_members_messaged_percent_trend": int(
                                          last_month_members_messaged_percent_trend),
                                      "last_month_members_messaged_trend_arrow": last_month_members_messaged_trend_arrow,
                                      "last_month_total_impressions": int(res_30["totalimpressions"]),
                                      "last_month_total_impressions_percent_trend": int(
                                          last_month_total_impressions_percent_trend),
                                      "last_month_total_impressions_trend_arrow": last_month_total_impressions_trend_arrow,
                                      "last_month_engagements": int(res_30["totalengagements"]),
                                      "last_month_engagements_percent_trend": int(last_month_engagements_percent_trend),
                                      "last_month_engagements_trend_arrow": last_month_engagements_trend_arrow,
                                      "last_month_engagements_rate_trend": round(res_30["engagementrate"], 1),
                                      "last_month_engagements_rate_percent_trend": round(
                                          last_month_engagements_rate_percent_trend, 1),
                                      "last_month_engagements_rate_trend_arrow": last_month_engagements_rate_trend_arrow,
                                      "last_90day_members_messaged": int(res_90["unique_members"]),
                                      "last_90day_members_messaged_percent_trend": int(
                                          last_90day_members_messaged_percent_trend),
                                      "last_90day_members_messaged_trend_arrow": last_90day_members_messaged_trend_arrow,
                                      "last_90day_total_impressions": int(res_90["totalimpressions"]),
                                      "last_90day_total_impressions_percent_trend": int(
                                          last_90day_total_impressions_percent_trend),
                                      "last_90day_total_impressions_trend_arrow": last_90day_total_impressions_trend_arrow,
                                      "last_90day_engagements": int(res_90["totalengagements"]),
                                      "last_90day_engagements_percent_trend": int(last_90day_engagements_percent_trend),
                                      "last_90day_engagements_trend_arrow": last_90day_engagements_trend_arrow,
                                      "last_90day_engagements_rate_trend": round(res_90["engagementrate"], 1),
                                      "last_90day_engagements_rate_percent_trend": round(
                                          last_90day_engagements_rate_percent_trend, 1),
                                      "last_90day_engagements_rate_trend_arrow": last_90day_engagements_rate_trend_arrow}

                            params_mr = {}
                            # Check if there are any records in messaging_reach table
                            sql_q = f"select count(*) from {RDS_MESSAGING_REACH_TABLE} where organization_id = :org_id"

                            params_mr["org_id"] = org_id
                            # params_mr["table_name"] = RDS_MESSAGING_REACH_TABLE
                            total_results = db.session.execute(sql_q, params_mr).scalar()

                            if total_results:
                                insert_update = "Update"
                            else:
                                insert_update = "Insert"
                            if insert_update == "Update":
                                try:
                                    upd_sql = f'UPDATE {RDS_MESSAGING_REACH_TABLE} ' \
                                              f'SET timestamp = :timestamp,last_week_members_messaged=:last_week_members_messaged,' \
                                              f' last_week_members_messaged_percent_trend=:last_week_members_messaged_percent_trend,' \
                                              f' last_week_members_messaged_trend_arrow=:last_week_members_messaged_trend_arrow, ' \
                                              f' last_week_total_impressions=:last_week_total_impressions, ' \
                                              f' last_week_total_impressions_percent_trend=:last_week_total_impressions_percent_trend, ' \
                                              f' last_week_total_impressions_trend_arrow=:last_week_total_impressions_trend_arrow, ' \
                                              f' last_week_engagements=:last_week_engagements, ' \
                                              f' last_week_engagements_percent_trend=:last_week_engagements_percent_trend, ' \
                                              f' last_week_engagements_trend_arrow=:last_week_engagements_trend_arrow, ' \
                                              f' last_week_engagements_rate_trend = :last_week_engagements_rate_trend, ' \
                                              f' last_week_engagements_rate_percent_trend = :last_week_engagements_rate_percent_trend, ' \
                                              f' last_week_engagements_rate_trend_arrow = :last_week_engagements_rate_trend_arrow,' \
                                              f' last_month_members_messaged=:last_month_members_messaged, ' \
                                              f' last_month_members_messaged_percent_trend=:last_month_members_messaged_percent_trend, ' \
                                              f' last_month_members_messaged_trend_arrow=:last_month_members_messaged_trend_arrow, ' \
                                              f' last_month_total_impressions=:last_month_total_impressions, ' \
                                              f' last_month_total_impressions_percent_trend=:last_month_total_impressions_percent_trend, ' \
                                              f' last_month_total_impressions_trend_arrow=:last_month_total_impressions_trend_arrow, ' \
                                              f' last_month_engagements=:last_month_engagements, ' \
                                              f' last_month_engagements_percent_trend=:last_month_engagements_percent_trend, ' \
                                              f' last_month_engagements_trend_arrow=:last_month_engagements_trend_arrow, ' \
                                              f' last_month_engagements_rate_trend = :last_month_engagements_rate_trend,' \
                                              f' last_month_engagements_rate_percent_trend = :last_month_engagements_rate_percent_trend,' \
                                              f' last_month_engagements_rate_trend_arrow = :last_month_engagements_rate_trend_arrow, ' \
                                              f' last_90day_members_messaged=:last_90day_members_messaged, ' \
                                              f' last_90day_members_messaged_percent_trend=:last_90day_members_messaged_percent_trend, ' \
                                              f' last_90day_members_messaged_trend_arrow=:last_90day_members_messaged_trend_arrow, ' \
                                              f' last_90day_total_impressions=:last_90day_total_impressions, ' \
                                              f' last_90day_total_impressions_percent_trend=:last_90day_total_impressions_percent_trend, ' \
                                              f' last_90day_total_impressions_trend_arrow=:last_90day_total_impressions_trend_arrow, ' \
                                              f' last_90day_engagements=:last_90day_engagements, ' \
                                              f' last_90day_engagements_percent_trend=:last_90day_engagements_percent_trend, ' \
                                              f' last_90day_engagements_trend_arrow=:last_90day_engagements_trend_arrow, ' \
                                              f'last_90day_engagements_rate_trend = :last_90day_engagements_rate_trend, ' \
                                              f'last_90day_engagements_rate_percent_trend = :last_90day_engagements_rate_percent_trend, ' \
                                              f'last_90day_engagements_rate_trend_arrow = :last_90day_engagements_rate_trend_arrow ' \
                                              f'WHERE organization_id=:organization_id'

                                    res = db.session.execute(upd_sql, params)
                                    db.session.commit()
                                    if res.rowcount > 0:
                                        insert_update = "UPDATED"
                                        msg = f'MYSQL UPDATE successful for org_id {org_id}'
                                        log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)
                                        total_updated += 1
                                except Exception as err:
                                    msg = f'MYSQL UPDATE EXCEPTION. UPDSQL={upd_sql} Params={params} Exception={str(err)}'
                                    log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)
                                    total_errors += 1
                                    # do not stop on exceptions, keep processing
                                    continue

                            # try to insert if update did not work or do anything
                            elif insert_update == 'Insert':
                                try:
                                    msg = f'MYSQL Before insert for org_id {org_id}'
                                    log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)

                                    ins_sql = f'INSERT INTO {RDS_MESSAGING_REACH_TABLE} ' \
                                              f'(timestamp,organization_id,last_week_members_messaged,last_week_members_messaged_percent_trend, last_week_members_messaged_trend_arrow,' \
                                              f'last_week_total_impressions,last_week_total_impressions_percent_trend,last_week_total_impressions_trend_arrow,last_week_engagements,' \
                                              f'last_week_engagements_percent_trend,last_week_engagements_trend_arrow,last_week_engagements_rate_trend,' \
                                              f'last_week_engagements_rate_percent_trend,last_week_engagements_rate_trend_arrow,' \
                                              f'last_month_members_messaged,last_month_members_messaged_percent_trend,' \
                                              f'last_month_members_messaged_trend_arrow,last_month_total_impressions,last_month_total_impressions_percent_trend ,' \
                                              f'last_month_total_impressions_trend_arrow,last_month_engagements,last_month_engagements_percent_trend,' \
                                              f'last_month_engagements_trend_arrow,last_month_engagements_rate_trend,last_month_engagements_rate_percent_trend,last_month_engagements_rate_trend_arrow,' \
                                              f'last_90day_members_messaged,last_90day_members_messaged_percent_trend ,last_90day_members_messaged_trend_arrow ,last_90day_total_impressions,' \
                                              f'last_90day_total_impressions_percent_trend,last_90day_total_impressions_trend_arrow ,last_90day_engagements ,' \
                                              f'last_90day_engagements_percent_trend ,last_90day_engagements_trend_arrow,' \
                                              f'last_90day_engagements_rate_trend,last_90day_engagements_rate_percent_trend,last_90day_engagements_rate_trend_arrow) ' \
                                              f'VALUES ' \
                                              f'(:timestamp,:organization_id,:last_week_members_messaged,:last_week_members_messaged_percent_trend, :last_week_members_messaged_trend_arrow,' \
                                              f':last_week_total_impressions,:last_week_total_impressions_percent_trend,:last_week_total_impressions_trend_arrow,:last_week_engagements,' \
                                              f':last_week_engagements_percent_trend,:last_week_engagements_trend_arrow,' \
                                              f':last_week_engagements_rate_trend,:last_week_engagements_rate_percent_trend,:last_week_engagements_rate_trend_arrow,' \
                                              f':last_month_members_messaged,:last_month_members_messaged_percent_trend,' \
                                              f':last_month_members_messaged_trend_arrow,:last_month_total_impressions,:last_month_total_impressions_percent_trend ,' \
                                              f':last_month_total_impressions_trend_arrow,:last_month_engagements,' \
                                              f':last_month_engagements_percent_trend,:last_month_engagements_trend_arrow,' \
                                              f':last_month_engagements_rate_trend,:last_month_engagements_rate_percent_trend,:last_month_engagements_rate_trend_arrow,' \
                                              f':last_90day_members_messaged,:last_90day_members_messaged_percent_trend ,:last_90day_members_messaged_trend_arrow ,:last_90day_total_impressions,' \
                                              f':last_90day_total_impressions_percent_trend,:last_90day_total_impressions_trend_arrow ,:last_90day_engagements ,' \
                                              f':last_90day_engagements_percent_trend ,:last_90day_engagements_trend_arrow,:last_90day_engagements_rate_trend,:last_90day_engagements_rate_percent_trend,:last_90day_engagements_rate_trend_arrow) '
                                    res = db.session.execute(ins_sql, params)
                                    db.session.commit()
                                    if res:
                                        insert_update = "INSERTED"
                                        total_inserted += 1
                                        msg = f'MYSQL Insert successful for org_id {org_id} {ins_sql} {params}'
                                        log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)
                                except Exception as err:
                                    msg = f'MYSQL INSERT EXCEPTION. INSSQL={ins_sql} Params={params} Exception={str(err)}'
                                    log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)
                                    total_errors += 1
                                    # do not stop on exceptions, keep processing
                                    continue

                            if insert_update == None:
                                total_errors += 1
                                msg = f'MYSQL Unable to insert/update record with Params={params}'
                                log_msg(SEVERITY_ERROR, FILE_NAME, FUNCTION_NAME, msg)
                                total_errors += 1
                            # commit every x records
                            elif (total_updated + total_inserted) % 250 == 0:
                                db.session.commit()

                            # Category overview
                            sql_query_params = {}

                            sql_query = f"WITH permissionrate AS (SELECT   udp.organization_id ," \
                                        f"Least(.9,(Sum(CASE WHEN udp.notification_permission = 'Accept' THEN 1 " \
                                        f"ELSE 0 END) / Count(udp.id))) permissionrate " \
                                        f"FROM     userdata_permissions udp " \
                                        f"WHERE    udp.notification_permission IS NOT NULL " \
                                        f"AND      udp.organization_id = :org_id " \
                                        f"AND      Date(udp.date) BETWEEN Curdate() - interval 30 day AND      curdate() - interval 1 day " \
                                        f"GROUP BY udp.organization_id ) , messagelist AS (" \
                                        f"SELECT     m.id                                       messageid ," \
                                        f"m.NAME                                     nudgename ," \
                                        f"m.organization_id                          orgid," \
                                        f"COALESCE(c.category_value,'Uncategorized') nudgecategory ," \
                                        f"ifnull(c.sort_order,9999)                  categorysortorder ," \
                                        f"min(ns.timestamp)                          firstsent ," \
                                        f"count(DISTINCT t.token)                    sentcount " \
                                        f"FROM       message m INNER JOIN nudges_sent ns ON         m.id = ns.message_id " \
                                        f"INNER JOIN token t ON         ns.device_id = t.device_id AND t.active = 1 and t.organization_id = :org_id " \
                                        f"LEFT JOIN  track_nudges_sent tns ON         tns.token = t.token " \
                                        f"AND        tns.nudge_sent_flag = 1 LEFT JOIN  categories c " \
                                        f"ON         m.message_category_id = c.category_key WHERE      m.organization_id = :org_id " \
                                        f"AND        c.category_value != 'Test nudge' " \
                                        f"AND        date(ns.timestamp) between curdate() - interval 30 day AND curdate() - interval 1 day " \
                                        f"GROUP BY   1,2,3), total_no AS (SELECT orgid," \
                                        f"sum(round(ml.sentcount * pr.permissionrate,0)) AS sumoftotal FROM   messagelist ml " \
                                        f"JOIN   permissionrate pr ON     pr.organization_id = ml.orgid) SELECT    ml.nudgecategory, " \
                                        f"sum(round(ml.sentcount * pr.permissionrate,0)) AS totalimpressionspermessage , " \
                                        f"total_no.sumoftotal ," \
                                        f"COALESCE((COALESCE(sum(round(ml.sentcount * pr.permissionrate,0)),0)/COALESCE(total_no.sumoftotal,0))*100,0) AS category_percent " \
                                        f"FROM      messagelist ml JOIN      permissionrate pr ON        pr.organization_id = ml.orgid " \
                                        f"JOIN      total_no ON        total_no.orgid = ml.orgid GROUP BY  1 ORDER BY  ml.orgid," \
                                        f"ml.categorysortorder,ml.nudgename"

                            sql_query_params["org_id"] = org_id
                            res = db.session.execute(sql_query, sql_query_params)
                            sql_results = res.fetchall()
                            if len(sql_results) != 0:
                                params_cr = {}
                                # Check if there are any records in messaging_reach table
                                sql_q = f"select count(*) from {RDS_CATEGORY_OVERVIEW_TABLE} where organization_id = :org_id"

                                params_cr["org_id"] = org_id
                                # params_cr["table_name"] = RDS_CATEGORY_OVERVIEW_TABLE
                                total_results = db.session.execute(sql_q, params_cr).scalar()

                                msg = f'count category overview org_id {total_results}'
                                log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)

                                if total_results > 0:
                                    sql_q = f"delete from {RDS_CATEGORY_OVERVIEW_TABLE} where organization_id = :org_id"
                                    params_q = {"org_id": org_id}
                                    res = db.session.execute(sql_q, params_q)
                                    db.session.commit()
                                    msg = f'inside delete {res}'
                                    log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)

                            for results in sql_results:
                                params = {'timestamp': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                                          'organization_id': org_id,
                                          'category_name': results["nudgecategory"],
                                          'category_percent_value': results["category_percent"],
                                          "category_total_impressions": results["totalimpressionspermessage"]
                                          }

                                sql_q = f"insert into  {RDS_CATEGORY_OVERVIEW_TABLE} (timestamp,organization_id," \
                                        f"category_name,category_percent_value,category_total_impressions) values " \
                                        f"(:timestamp,:organization_id,:category_name,:category_percent_value,:category_total_impressions)"
                                res = db.session.execute(sql_q, params)
                                db.session.commit()

                            # Insights
                            data = tuple()
                            sql_query = f"WITH permissionrate AS (SELECT   udp.organization_id ," \
                                        f"Least(.9,(Sum(CASE " \
                                        f"WHEN udp.notification_permission = 'Accept' THEN 1 ELSE 0 END) / Count(udp.id))) permissionrate " \
                                        f"FROM     userdata_permissions udp WHERE    udp.notification_permission IS NOT NULL " \
                                        f"AND      udp.organization_id = :org_id " \
                                        f"AND      Date(udp.date) between Curdate() - interval 30 day AND curdate() - interval 1 day " \
                                        f"GROUP BY udp.organization_id ) ,nudgesreceived AS (" \
                                        f"SELECT     nr.message_id                             messageid ," \
                                        f"round(count(nr.id) * pr.permissionrate,0) receivedcount " \
                                        f"FROM       nudges_sent ns " \
                                        f"INNER JOIN nudges_received nr ON ns.id = nr.nudge_sent_id " \
                                        f"AND  nr.nudge_sent_id_code in (1,4) AND nr.organization_id = :org_id " \
                                        f"INNER JOIN token t on t.device_id = nr.device_id and t.active = 1 and platform = 'ios' and t.organization_id = :org_id " \
                                        f"JOIN       permissionrate pr ON         pr.organization_id = nr.organization_id " \
                                        f"WHERE      nr.organization_id = :org_id " \
                                        f"AND        date(nr.timestamp) between Curdate() - interval 30 day AND curdate() - interval 1 day " \
                                        f"GROUP BY   nr.message_id) ,nudgestapped AS (SELECT     nt.message_id messageid ," \
                                        f"count(nt.id)  tappedcount FROM       nudges_sent ns " \
                                        f"INNER JOIN nudges_tapped nt ON ns.id = nt.nudge_sent_id AND nt.nudge_sent_id_code in (1,4) " \
                                        f"INNER JOIN token t on t.device_id = nt.device_id and t.active = 1 and platform = 'ios' and t.organization_id = :org_id " \
                                        f"WHERE      nt.organization_id = :org_id " \
                                        f"AND        date(ns.timestamp) between Curdate() - interval 30 day AND curdate() - interval 1 day " \
                                        f"GROUP BY   nt.message_id ) , messagelist AS (" \
                                        f"SELECT     o.id                                       orgid ," \
                                        f"o.NAME                                     orgname , " \
                                        f"a.id                                       actionid ," \
                                        f"cn.campaign_id                             campaign_id," \
                                        f"m.id                                       messageid ," \
                                        f"m.NAME                                     nudgename ," \
                                        f"COALESCE(c.category_value,'Uncategorized') nudgecategory ," \
                                        f"ifnull(c.sort_order,9999)                  categorysortorder ," \
                                        f"min(ns.timestamp)                          firstsent ," \
                                        f"count(DISTINCT tns.token)                    sentcount," \
                                        f"ns.nudge_type FROM       message m INNER JOIN action a ON " \
                                        f"a.message_id = m.id and a.action_type_id = 1 LEFT JOIN  campaign_nudges cn " \
                                        f"ON         cn.nudge_id = a.id INNER JOIN nudges_sent ns " \
                                        f"ON         m.id = ns.message_id INNER JOIN token t " \
                                        f"ON         ns.device_id = t.device_id AND        t.active = 1 " \
                                        f"INNER JOIN  track_nudges_sent tns ON  tns.nudge_sent_id = ns.id " \
                                        f"AND        tns.nudge_sent_flag = 1 INNER JOIN organization o ON         m.organization_id = o.id " \
                                        f"LEFT JOIN  categories c ON         m.message_category_id = c.category_key " \
                                        f"WHERE      m.organization_id = :org_id " \
                                        f"AND        date(ns.timestamp) between Curdate() - interval 30 day AND curdate() - interval 1 day " \
                                        f"GROUP BY   1,2,3,4 ) SELECT    ml.nudgecategory, ml.messageid,ml.nudgename," \
                                        f"ml.firstsent,ml.actionid,ml.campaign_id," \
                                        f"concat('The ', ml.nudgename,' nudge has an engagement rate of ',ifnull(round(ifnull(nt.tappedcount,0)/ifnull(nr.receivedcount,0),1),0) * 100,'%.') AS string_text , " \
                                        f"round(ml.sentcount * pr.permissionrate,0) impressions ," \
                                        f"ifnull(round(ifnull(nt.tappedcount,0)/ifnull(nr.receivedcount,0),1),0)*100  engagementrate ,ml.nudge_type " \
                                        f"FROM      messagelist ml JOIN      permissionrate pr ON        pr.organization_id = ml.orgid " \
                                        f"LEFT JOIN nudgesreceived nr ON        nr.messageid = ml.messageid LEFT JOIN nudgestapped nt " \
                                        f"ON        nt.messageid = ml.messageid WHERE     ml.nudgecategory != 'Test nudge' " \
                                        f"ORDER BY  ml.orgname, ml.categorysortorder,ml.nudgename"

                            sql_query_params["org_id"] = org_id
                            res = db.session.execute(sql_query, sql_query_params)
                            sql_results = res.fetchall()
                            counter_high_nudges = 0
                            counter_high_campaigns = 0
                            counter_low_nudges = 0
                            counter_low_campaigns = 0
                            val_list = []
                            msg = f'starting insights org_id {org_id}'
                            log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)

                            # Loop through the result set of nudges with impressions >= 100 and capture each of 3 low engagement nudges, low campaigns
                            # 3 high engagement nudges  and 3 high engagement campaigns,
                            for results in sql_results:
                                url = app.config.get("NUDGE_DASHBOARD_URL")
                                if results["nudge_type"] == 1 and results["engagementrate"] >= 15 and results[
                                    "impressions"] >= 100 and counter_high_campaigns < 3:
                                    title = "High engagement campaign"
                                    Members_Messaged = 0
                                    Total_Impressions = int(results["impressions"])
                                    Engagements = 0
                                    description = results["string_text"]
                                    icon = INSIGHT_ICON_HIGH_ENGAGEMENT_CAMPAIGN
                                    url = '/campaigns/edit/' + str(results["campaign_id"])
                                    ranking = 3
                                    val_tuple = (
                                        datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'), org_id, title,
                                        Members_Messaged,
                                        Total_Impressions, Engagements, description, icon, url, ranking)
                                    val_list.append(val_tuple)
                                    counter_high_campaigns += 1
                                elif results["nudge_type"] == 1 and results["engagementrate"] <= 3 and results[
                                    "impressions"] >= 100 and counter_low_campaigns < 3:
                                    title = "Low engagement campaign"
                                    Members_Messaged = 0
                                    Total_Impressions = int(results["impressions"])
                                    Engagements = 0
                                    description = results["string_text"]
                                    icon = INSIGHT_ICON_LOW_ENGAGEMENT_CAMPAIGN
                                    url = '/campaigns/edit/' + str(results["campaign_id"])
                                    ranking = 5
                                    val_tuple = (
                                        datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'), org_id, title,
                                        Members_Messaged,
                                        Total_Impressions, Engagements, description, icon, url, ranking)
                                    val_list.append(val_tuple)
                                    counter_low_campaigns += 1
                                elif results["nudge_type"] != 1 and results["engagementrate"] >= 15 and results[
                                    "impressions"] >= 100 and counter_high_nudges <= 3:
                                    title = "High engagement nudges"
                                    Members_Messaged = 0
                                    Total_Impressions = int(results["impressions"])
                                    Engagements = 0
                                    description = results["string_text"]
                                    icon = INSIGHT_ICON_HIGH_ENGAGEMENT_NUDGE
                                    url = '/nudges/edit/' + str(results["actionid"])
                                    ranking = 2
                                    val_tuple = (
                                        datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'), org_id, title,
                                        Members_Messaged,
                                        Total_Impressions, Engagements, description, icon, url, ranking)
                                    val_list.append(val_tuple)
                                    counter_high_nudges += 1
                                elif results["nudge_type"] != 1 and results["engagementrate"] <= 3 and results[
                                    "impressions"] >= 100 and counter_low_nudges < 3:
                                    title = "Low engagement nudges"
                                    Members_Messaged = 0
                                    Total_Impressions = int(results["impressions"])
                                    Engagements = 0
                                    description = results["string_text"]
                                    icon = INSIGHT_ICON_LOW_ENGAGEMENT_NUDGE
                                    url = '/nudges/edit/' + str(results["actionid"])
                                    ranking = 4

                                    val_tuple = (
                                        datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'), org_id, title,
                                        Members_Messaged,
                                        Total_Impressions, Engagements, description, icon, url, ranking)
                                    val_list.append(val_tuple)
                                    counter_low_nudges += 1

                            params_insights = {}
                            # Check if there are any records in messaging_reach table
                            sql_q = f"select count(*) from {RDS_INSIGHTS_TABLE} where organization_id = :org_id"

                            params_insights["org_id"] = org_id
                            # params_insights["table_name"] = RDS_INSIGHTS_TABLE
                            total_results = db.session.execute(sql_q, params_insights).scalar()

                            msg = f'count insights org_id {total_results}'
                            log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)

                            if total_results > 0:
                                sql_q = f"delete from {RDS_INSIGHTS_TABLE} where organization_id = :org_id"
                                params_q = {"org_id": org_id}
                                res = db.session.execute(sql_q, params_q)
                                db.session.commit()
                                msg = f'inside delete {res}'
                                log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)

                            msg = f'Before MYSQL Insert {RDS_INSIGHTS_TABLE} org_id {org_id} in {(val_list)}'
                            log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)

                            if len(val_list) > 0:
                                values = ', '.join(map(str, val_list))
                                sql = "INSERT INTO analytics_insights_by_org (timestamp,organization_id,title,Members_Messaged," \
                                      "Total_Impressions,Engagements,description,icon,url,ranking) VALUES {}".format(
                                    values)
                                msg = f'MYSQL Insert {RDS_INSIGHTS_TABLE} org_id {org_id} in {sql}'
                                log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)
                                db.session.execute(sql)
                                db.session.commit()
                                msg = f'MYSQL Insert successful for {RDS_INSIGHTS_TABLE} org_id {org_id} in {RDS_INSIGHTS_TABLE}'
                                log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)

                        except Exception as err:
                            msg = f'Exception in individual org_id {org_id}'
                            log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)
                            total_errors += 1
                            # do not stop on exceptions, keep processing
                            continue

                        num_orgs += 1

                    timer.stop()
                    msg = f" Calculated {num_orgs} orgs for Messaging Reach, insights and category overview."
                    log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)

                    publish_audit_event(AuditEvents.AUDIT_EVENT_NUDGE_MESSAGING_REACH,
                                        AuditOps.AUDIT_OP_STOP,
                                        total_errors,
                                        additional_msg=msg,
                                        elapsed_secs=timer.elapsed_time())

                    return 201

                except Exception as err:
                    timer.stop()
                    msg = f"GENERAL EXCEPTION: TASK_ID:{task_id} calculating analytics messaging reach query  Ex: {str(err)}"
                    log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)
                    return msg

    except Exception as err:
        timer.stop()
        msg = f"GENERAL EXCEPTION: TASK_ID:{task_id} calculating analytics messaging reach query  Ex: {str(err)}"
        log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)
        raise err


@celery_app.task(bind=True, acks_late=True)
def calc_nudge_analytics_delay(self):
    """
        Function to caluclate individual nudge analytics data asynchronously
        :return: 201
    """
    from core.views.analytics_dashboard import messaging_reach_query
    FUNCTION_NAME = "calc_nudge_analytics_delay"
    RDS_NUDGE_ANALYTICS = "nudge_analytics"
    total_updated = 0
    total_inserted = 0
    total_errors = 0
    insert_update = None
    num_orgs = 0
    timer = Timer()
    timer.start()
    try:
        from core.app import create_app
        with create_app().app_context():
            with app.test_request_context('/analytics-dashboard/larkydr.internal'):
                try:
                    task_id = self.request.id
                    msg = f"TASK_ID:{task_id} Start calculating individual nudge analytics"
                    log_msg(SEVERITY_INFO, FILE_NAME, FUNCTION_NAME, msg)

                    db_name = app.config.get('DATABASE_NAME', 'microservicedb')

                    # Update recalc flag for nudges whose end date is < utc_timestamp()
                    sql_upd_str = (
                        f"update nudge_analytics na inner join action a on na.message_id = a.message_id and a.action_type_id = 1 "
                        f"left join microservicedb.condition c on c.action_id = a.id and c.condition_type_id = 7 "
                        f"left join (select cn.campaign_id campaign_id,cn.nudge_id,campaign.start_date as campaign_start_date, "
                        f"campaign.end_date as campaign_end_date from campaign inner join campaign_nudges cn on campaign.id = cn.campaign_id) "
                        f"camp_nudges on a.id = camp_nudges.nudge_id set recalc_flag = 1, timestamp = utc_timestamp(), is_final_flag = 1 "
                        f"where coalesce(c.date_time_range_end,camp_nudges.campaign_end_date) < utc_timestamp() and coalesce(is_final_flag,0) != 1")

                    result_upd = db.session.execute(sql_upd_str)
                    msg = f"TASK_ID:{task_id} Updating recalc flag for ending nudges {result_upd}"
                    log_msg(SEVERITY_INFO, FILE_NAME, FUNCTION_NAME, msg)
                    # get the message_id to be re-calculated. Get details like start_date and end_date
                    sql_str = (f'select na.organization_id,na.message_id,na.nudge_type,na.timestamp,a.id as action_id, '
                               f'coalesce(c.date_time_range_end,camp_nudges.campaign_end_date),coalesce(c.date_time_range_start,'
                               f'camp_nudges.campaign_start_date),coalesce(camp_nudges.campaign_id,0) as campaign_id '
                               f'from nudge_analytics na inner join organization o on o.id = na.organization_id '
                               f'inner join organization_type ot on o.organization_type_id = ot.id and o.active = 1 and ot.calculate_analytics = 1 '
                               f'inner join action a on a.message_id = na.message_id and a.action_type_id = 1 '
                               f'left join {db_name}.condition c on c.action_id = a.id and c.condition_type_id = 7 '
                               f'left join (select cn.campaign_id campaign_id,cn.nudge_id,campaign.start_date as campaign_start_date, '
                               f'campaign.end_date as campaign_end_date from campaign '
                               f'inner join campaign_nudges cn on campaign.id = cn.campaign_id) camp_nudges '
                               f'on a.id = camp_nudges.nudge_id where recalc_flag = 1 order by na.message_id desc')
                    # Run the SQL
                    result = db.session.execute(sql_str)

                    for row in result:
                        try:
                            org_id = row[0]
                            message_id = row[1]
                            nudge_type = row[2]
                            timestamp = row[3]
                            action_id = row[4]
                            nudge_end_date = row[5]
                            nudge_start_date = row[6]
                            campaign_id = row[7]
                            msg = f"TASK_ID:{task_id} Start calculating nudge analytics org_id {org_id} for message_id {message_id}"
                            log_msg(SEVERITY_INFO, FILE_NAME, FUNCTION_NAME, msg)

                            # Check if nudge has been completed to turn on is_final_flag and turn on 30_day_analytics flag if nudge is more than 30 days old
                            recalc_flag = 0
                            day_30_date = None
                            current_utc_date = datetime.strptime(datetime.utcnow().strftime('%m/%d/%y'), '%m/%d/%y')
                            if nudge_end_date:
                                nudge_end_date = datetime.strptime(nudge_end_date.strftime('%m/%d/%y'), '%m/%d/%y')
                                is_final_flag = 1 if current_utc_date > nudge_end_date else 0
                            else:
                                is_final_flag = 0
                            if nudge_start_date:
                                day_30_date = nudge_start_date + timedelta(days=30)
                                day_30_date = datetime.strptime(day_30_date.strftime('%m/%d/%y'), '%m/%d/%y')
                            if day_30_date:
                                day_30_analytics_flag = 1 if datetime.strptime(current_utc_date.strftime('%m/%d/%y'),
                                                                               '%m/%d/%y') >= day_30_date else 0
                            else:
                                day_30_analytics_flag = 0

                            if nudge_type != 8:
                                interval_high_180 = 180
                                interval_low_91 = 91
                                interval_high_90 = 90
                                interval_low_90 = 0

                                interval_high_60 = 60
                                interval_low_31 = 31
                                interval_high_30 = 30
                                interval_low_30 = 0

                                interval_high_14 = 14
                                interval_low_8 = 8
                                interval_high_7 = 7
                                interval_low_7 = 0

                                if nudge_end_date:
                                    if current_utc_date > nudge_end_date:
                                        interval_high_date_180 = nudge_end_date - timedelta(days=180)
                                        interval_high_dict_180 = current_utc_date - interval_high_date_180
                                        interval_high_180 = interval_high_dict_180.days

                                        interval_low_date_91 = nudge_end_date - timedelta(days=91)
                                        interval_low_dict_91 = current_utc_date - interval_low_date_91
                                        interval_low_91 = interval_low_dict_91.days

                                        interval_high_date_90 = nudge_end_date - timedelta(days=90)
                                        interval_high_dict_90 = current_utc_date - interval_high_date_90
                                        interval_high_90 = interval_high_dict_90.days

                                        interval_low_dict_90 = current_utc_date - nudge_end_date
                                        interval_low_90 = interval_low_dict_90.days

                                        interval_high_date_60 = nudge_end_date - timedelta(days=60)
                                        interval_high_dict_60 = current_utc_date - interval_high_date_60
                                        interval_high_60 = interval_high_dict_60.days

                                        interval_low_date_31 = nudge_end_date - timedelta(days=31)
                                        interval_low_dict_31 = current_utc_date - interval_low_date_31
                                        interval_low_31 = interval_low_dict_31.days

                                        interval_high_date_30 = nudge_end_date - timedelta(days=30)
                                        interval_high_dict_30 = current_utc_date - interval_high_date_30
                                        interval_high_30 = interval_high_dict_30.days

                                        interval_low_dict_30 = current_utc_date - nudge_end_date
                                        interval_low_30 = interval_low_dict_30.days

                                        interval_high_date_14 = nudge_end_date - timedelta(days=14)
                                        interval_high_dict_14 = current_utc_date - interval_high_date_14
                                        interval_high_14 = interval_high_dict_14.days

                                        interval_low_date_8 = nudge_end_date - timedelta(days=8)
                                        interval_low_dict_8 = current_utc_date - interval_low_date_8
                                        interval_low_8 = interval_low_dict_8.days

                                        interval_high_date_7 = nudge_end_date - timedelta(days=7)
                                        interval_high_dict_7 = current_utc_date - interval_high_date_7
                                        interval_high_7 = interval_high_dict_7.days

                                        interval_low_dict_7 = current_utc_date - nudge_end_date
                                        interval_low_7 = interval_low_dict_7.days

                                res_180 = messaging_reach_query(org_id=org_id, interval_high=interval_high_180,
                                                                interval_low=interval_low_91, nudge_today=0,
                                                                message_id=message_id, nudge_type=nudge_type)[0]
                                res_90 = messaging_reach_query(org_id=org_id, interval_high=interval_high_90,
                                                               interval_low=interval_low_90, nudge_today=0,
                                                               message_id=message_id, nudge_type=nudge_type)[0]

                                msg = f"TASK_ID:{task_id} Inside calculating nudge analytics org_id {org_id} for message_id {message_id}"
                                log_msg(SEVERITY_INFO, FILE_NAME, FUNCTION_NAME, msg)

                                # Trend and percent calculation
                                result_trends_90 = calc_trend_count(res_90["unique_members"], res_180["unique_members"],
                                                                    res_90["totalimpressions"],
                                                                    res_180["totalimpressions"],
                                                                    res_90["totalengagements"],
                                                                    res_180["totalengagements"],
                                                                    res_90["engagementrate"], res_180["engagementrate"])

                                msg = f"TASK_ID:{task_id} After calculating calc_trend_count {result_trends_90} for message_id {message_id}"
                                log_msg(SEVERITY_INFO, FILE_NAME, FUNCTION_NAME, msg)

                                last_90day_members_messaged_percent_trend = result_trends_90[0]
                                last_90day_members_messaged_trend_arrow = result_trends_90[1]
                                last_90day_total_impressions_percent_trend = result_trends_90[2]
                                last_90day_total_impressions_trend_arrow = result_trends_90[3]
                                last_90day_engagements_percent_trend = result_trends_90[4]
                                last_90day_engagements_trend_arrow = result_trends_90[5]
                                last_90day_engagements_rate_percent_trend = result_trends_90[6]
                                last_90day_engagements_rate_trend_arrow = result_trends_90[7]

                                res_60 = messaging_reach_query(org_id=org_id, interval_high=interval_high_60,
                                                               interval_low=interval_low_31, nudge_today=0,
                                                               message_id=message_id, nudge_type=nudge_type)[0]
                                res_30 = messaging_reach_query(org_id=org_id, interval_high=interval_high_30,
                                                               interval_low=interval_low_30, nudge_today=0,
                                                               message_id=message_id, nudge_type=nudge_type)[0]

                                # Trend and percent calculation
                                result_trends_30 = calc_trend_count(res_30["unique_members"], res_60["unique_members"],
                                                                    res_30["totalimpressions"],
                                                                    res_60["totalimpressions"],
                                                                    res_30["totalengagements"],
                                                                    res_60["totalengagements"],
                                                                    res_30["engagementrate"], res_60["engagementrate"])

                                last_month_members_messaged_percent_trend = result_trends_30[0]
                                last_month_members_messaged_trend_arrow = result_trends_30[1]
                                last_month_total_impressions_percent_trend = result_trends_30[2]
                                last_month_total_impressions_trend_arrow = result_trends_30[3]
                                last_month_engagements_percent_trend = result_trends_30[4]
                                last_month_engagements_trend_arrow = result_trends_30[5]
                                last_month_engagements_rate_percent_trend = result_trends_30[6]
                                last_month_engagements_rate_trend_arrow = result_trends_30[7]

                                res_14 = messaging_reach_query(org_id=org_id, interval_high=interval_high_14,
                                                               interval_low=interval_low_8, nudge_today=0,
                                                               message_id=message_id, nudge_type=nudge_type)[0]
                                res_7 = messaging_reach_query(org_id=org_id, interval_high=interval_high_7,
                                                              interval_low=interval_low_7, nudge_today=0,
                                                              message_id=message_id, nudge_type=nudge_type)[0]

                                # Trend and percent calculation
                                result_trends_7 = calc_trend_count(res_7["unique_members"], res_14["unique_members"],
                                                                   res_7["totalimpressions"],
                                                                   res_14["totalimpressions"],
                                                                   res_7["totalengagements"],
                                                                   res_14["totalengagements"],
                                                                   res_7["engagementrate"], res_14["engagementrate"])

                                last_week_members_messaged_percent_trend = result_trends_7[0]
                                last_week_members_messaged_trend_arrow = result_trends_7[1]
                                last_week_total_impressions_percent_trend = result_trends_7[2]
                                last_week_total_impressions_trend_arrow = result_trends_7[3]
                                last_week_engagements_percent_trend = result_trends_7[4]
                                last_week_engagements_trend_arrow = result_trends_7[5]
                                last_week_engagements_rate_percent_trend = result_trends_7[6]
                                last_week_engagements_rate_trend_arrow = result_trends_7[7]

                                # if it's a campaign nudge, calculate the overall analytics and update into unique recipients
                                # , total impressions, engagements and engagement rates

                                interval_high = 0
                                if nudge_start_date:
                                    campaign_start_date = datetime.strptime(nudge_start_date.strftime('%m/%d/%y'),
                                                                            '%m/%d/%y')
                                    interval_high_dict = current_utc_date - campaign_start_date
                                    interval_high = interval_high_dict.days

                                interval_low = 0
                                if nudge_end_date:
                                    if current_utc_date > nudge_end_date:
                                        campaign_end_date = datetime.strptime(nudge_end_date.strftime('%m/%d/%y'),
                                                                              '%m/%d/%y')
                                        interval_low_dict = current_utc_date - campaign_end_date
                                        interval_low = interval_low_dict.days

                                msg = f'message_id {message_id} is a nudge_type={nudge_type} with interval_high = {interval_high} and interval_low = {interval_low}'
                                log_msg(SEVERITY_INFO, FILE_NAME, FUNCTION_NAME, msg)

                                overall_camp_res = messaging_reach_query(org_id=org_id, interval_high=interval_high,
                                                                         interval_low=interval_low, nudge_today=0,
                                                                         nudge_type=nudge_type, message_id=message_id)[
                                    0]
                                unique_recipients = int(overall_camp_res["unique_members"])
                                total_impressions = int(overall_camp_res["totalimpressions"])
                                engagements = int(overall_camp_res["totalengagements"])
                                engagements_rate = round(overall_camp_res["engagementrate"], 1)

                                # if nudge_type == 1:
                                #
                                #     interval_high = 0
                                #     if nudge_start_date:
                                #         campaign_start_date = datetime.strptime(nudge_start_date.strftime('%m/%d/%y'),'%m/%d/%y')
                                #         interval_high_dict = current_utc_date - campaign_start_date
                                #         interval_high = interval_high_dict.days
                                #
                                #
                                #     interval_low = 0
                                #     if nudge_end_date:
                                #         if current_utc_date > nudge_end_date:
                                #             campaign_end_date = datetime.strptime(nudge_end_date.strftime('%m/%d/%y'),'%m/%d/%y')
                                #             interval_low_dict = current_utc_date - campaign_end_date
                                #             interval_low = interval_low_dict.days
                                #
                                #     msg = f'message_id {message_id} is a campaign with interval_high = {interval_high} and interval_low = {interval_low}'
                                #     log_msg(SEVERITY_INFO, FILE_NAME, FUNCTION_NAME, msg)
                                #
                                #     overall_camp_res = messaging_reach_query(org_id=org_id, interval_high=interval_high, interval_low=interval_low, nudge_today=0,nudge_type=nudge_type, message_id=message_id)[0]
                                #     unique_recipients = int(overall_camp_res["unique_members"])
                                #     total_impressions = int(overall_camp_res["totalimpressions"])
                                #     engagements = int(overall_camp_res["totalengagements"])
                                #     engagements_rate = round(overall_camp_res["engagementrate"],1)
                                # else:
                                #     res_full = messaging_reach_query(org_id=org_id, interval_high=interval_high_14,
                                #                                    interval_low=interval_low_8, nudge_today=0,
                                #                                    message_id=message_id, nudge_type=nudge_type)[0]
                                #
                                #     unique_recipients = int(res_full["unique_members"])
                                #     total_impressions = int(res_full["totalimpressions"])
                                #     engagements = int(res_full["totalengagements"])
                                #     engagements_rate = round(res_full["engagementrate"],1)
                                # try to update 1st
                                params = {"timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                                          "organization_id": org_id,
                                          "message_id": message_id, "nudge_type": nudge_type,
                                          "recalc_flag": recalc_flag,
                                          "unique_recipients": unique_recipients,
                                          "total_impressions": total_impressions,
                                          "engagements": engagements, "engagements_rate": engagements_rate,
                                          "is_final_flag": is_final_flag,
                                          "day_30_analytics_flag": day_30_analytics_flag,
                                          "last_week_members_messaged": int(res_7["unique_members"]),
                                          "last_week_members_messaged_percent_trend": int(
                                              last_week_members_messaged_percent_trend),
                                          "last_week_members_messaged_trend_arrow": last_week_members_messaged_trend_arrow,
                                          "last_week_total_impressions": int(res_7["totalimpressions"]),
                                          "last_week_total_impressions_percent_trend": int(
                                              last_week_total_impressions_percent_trend),
                                          "last_week_total_impressions_trend_arrow": last_week_total_impressions_trend_arrow,
                                          "last_week_engagements": int(res_7["totalengagements"]),
                                          "last_week_engagements_percent_trend": int(
                                              last_week_engagements_percent_trend),
                                          "last_week_engagements_trend_arrow": last_week_engagements_trend_arrow,
                                          "last_week_engagements_rate_trend": round(res_7["engagementrate"], 1),
                                          "last_week_engagements_rate_percent_trend": round(
                                              last_week_engagements_rate_percent_trend, 1),
                                          "last_week_engagements_rate_trend_arrow": last_week_engagements_rate_trend_arrow,
                                          "last_month_members_messaged": int(res_30["unique_members"]),
                                          "last_month_members_messaged_percent_trend": int(
                                              last_month_members_messaged_percent_trend),
                                          "last_month_members_messaged_trend_arrow": last_month_members_messaged_trend_arrow,
                                          "last_month_total_impressions": int(res_30["totalimpressions"]),
                                          "last_month_total_impressions_percent_trend": int(
                                              last_month_total_impressions_percent_trend),
                                          "last_month_total_impressions_trend_arrow": last_month_total_impressions_trend_arrow,
                                          "last_month_engagements": int(res_30["totalengagements"]),
                                          "last_month_engagements_percent_trend": int(
                                              last_month_engagements_percent_trend),
                                          "last_month_engagements_trend_arrow": last_month_engagements_trend_arrow,
                                          "last_month_engagements_rate_trend": round(res_30["engagementrate"], 1),
                                          "last_month_engagements_rate_percent_trend": round(
                                              last_month_engagements_rate_percent_trend, 1),
                                          "last_month_engagements_rate_trend_arrow": last_month_engagements_rate_trend_arrow,
                                          "last_90day_members_messaged": int(res_90["unique_members"]),
                                          "last_90day_members_messaged_percent_trend": int(
                                              last_90day_members_messaged_percent_trend),
                                          "last_90day_members_messaged_trend_arrow": last_90day_members_messaged_trend_arrow,
                                          "last_90day_total_impressions": int(res_90["totalimpressions"]),
                                          "last_90day_total_impressions_percent_trend": int(
                                              last_90day_total_impressions_percent_trend),
                                          "last_90day_total_impressions_trend_arrow": last_90day_total_impressions_trend_arrow,
                                          "last_90day_engagements": int(res_90["totalengagements"]),
                                          "last_90day_engagements_percent_trend": int(
                                              last_90day_engagements_percent_trend),
                                          "last_90day_engagements_trend_arrow": last_90day_engagements_trend_arrow,
                                          "last_90day_engagements_rate_trend": round(res_90["engagementrate"], 1),
                                          "last_90day_engagements_rate_percent_trend": round(
                                              last_90day_engagements_rate_percent_trend, 1),
                                          "last_90day_engagements_rate_trend_arrow": last_90day_engagements_rate_trend_arrow}

                                params_mr = {}
                                # Check if there are any records in messaging_reach table
                                sql_q = f"select count(*) from {RDS_NUDGE_ANALYTICS} where organization_id = :org_id " \
                                        f"and message_id = :message_id"

                                params_mr["org_id"] = org_id
                                params_mr["message_id"] = message_id
                                total_results = db.session.execute(sql_q, params_mr).scalar()

                                if total_results:
                                    insert_update = "Update"
                                else:
                                    insert_update = "Insert"
                                if insert_update == "Update":
                                    try:
                                        upd_sql = f'UPDATE {RDS_NUDGE_ANALYTICS} ' \
                                                  f'SET timestamp = :timestamp,last_week_members_messaged=:last_week_members_messaged,' \
                                                  f' recalc_flag = :recalc_flag, is_final_flag=:is_final_flag, day_30_analytics_flag = :day_30_analytics_flag, ' \
                                                  f' unique_recipients = :unique_recipients,total_impressions = :total_impressions,engagements = :engagements, ' \
                                                  f' engagements_rate = :engagements_rate, ' \
                                                  f' last_week_members_messaged_percent_trend=:last_week_members_messaged_percent_trend,' \
                                                  f' last_week_members_messaged_trend_arrow=:last_week_members_messaged_trend_arrow, ' \
                                                  f' last_week_total_impressions=:last_week_total_impressions, ' \
                                                  f' last_week_total_impressions_percent_trend=:last_week_total_impressions_percent_trend, ' \
                                                  f' last_week_total_impressions_trend_arrow=:last_week_total_impressions_trend_arrow, ' \
                                                  f' last_week_engagements=:last_week_engagements, ' \
                                                  f' last_week_engagements_percent_trend=:last_week_engagements_percent_trend, ' \
                                                  f' last_week_engagements_trend_arrow=:last_week_engagements_trend_arrow, ' \
                                                  f' last_week_engagements_rate_trend = :last_week_engagements_rate_trend, ' \
                                                  f' last_week_engagements_rate_percent_trend = :last_week_engagements_rate_percent_trend, ' \
                                                  f' last_week_engagements_rate_trend_arrow = :last_week_engagements_rate_trend_arrow,' \
                                                  f' last_month_members_messaged=:last_month_members_messaged, ' \
                                                  f' last_month_members_messaged_percent_trend=:last_month_members_messaged_percent_trend, ' \
                                                  f' last_month_members_messaged_trend_arrow=:last_month_members_messaged_trend_arrow, ' \
                                                  f' last_month_total_impressions=:last_month_total_impressions, ' \
                                                  f' last_month_total_impressions_percent_trend=:last_month_total_impressions_percent_trend, ' \
                                                  f' last_month_total_impressions_trend_arrow=:last_month_total_impressions_trend_arrow, ' \
                                                  f' last_month_engagements=:last_month_engagements, ' \
                                                  f' last_month_engagements_percent_trend=:last_month_engagements_percent_trend, ' \
                                                  f' last_month_engagements_trend_arrow=:last_month_engagements_trend_arrow, ' \
                                                  f' last_month_engagements_rate_trend = :last_month_engagements_rate_trend,' \
                                                  f' last_month_engagements_rate_percent_trend = :last_month_engagements_rate_percent_trend,' \
                                                  f' last_month_engagements_rate_trend_arrow = :last_month_engagements_rate_trend_arrow, ' \
                                                  f' last_90day_members_messaged=:last_90day_members_messaged, ' \
                                                  f' last_90day_members_messaged_percent_trend=:last_90day_members_messaged_percent_trend, ' \
                                                  f' last_90day_members_messaged_trend_arrow=:last_90day_members_messaged_trend_arrow, ' \
                                                  f' last_90day_total_impressions=:last_90day_total_impressions, ' \
                                                  f' last_90day_total_impressions_percent_trend=:last_90day_total_impressions_percent_trend, ' \
                                                  f' last_90day_total_impressions_trend_arrow=:last_90day_total_impressions_trend_arrow, ' \
                                                  f' last_90day_engagements=:last_90day_engagements, ' \
                                                  f' last_90day_engagements_percent_trend=:last_90day_engagements_percent_trend, ' \
                                                  f' last_90day_engagements_trend_arrow=:last_90day_engagements_trend_arrow, ' \
                                                  f'last_90day_engagements_rate_trend = :last_90day_engagements_rate_trend, ' \
                                                  f'last_90day_engagements_rate_percent_trend = :last_90day_engagements_rate_percent_trend, ' \
                                                  f'last_90day_engagements_rate_trend_arrow = :last_90day_engagements_rate_trend_arrow ' \
                                                  f'WHERE organization_id=:organization_id and message_id = :message_id and nudge_type = :nudge_type'

                                        res = db.session.execute(upd_sql, params)
                                        db.session.commit()
                                        if res.rowcount > 0:
                                            insert_update = "UPDATED"
                                            msg = f'MYSQL UPDATE successful for org_id {org_id}'
                                            log_msg(SEVERITY_INFO, FILE_NAME, FUNCTION_NAME, msg)
                                            total_updated += 1
                                    except Exception as err:
                                        msg = f'MYSQL UPDATE EXCEPTION. UPDSQL={upd_sql} Params={params} Exception={str(err)}'
                                        log_msg(SEVERITY_INFO, FILE_NAME, FUNCTION_NAME, msg)
                                        total_errors += 1
                                        # do not stop on exceptions, keep processing
                                        continue

                                # try to insert if update did not work or do anything
                                elif insert_update == 'Insert':
                                    try:
                                        msg = f'MYSQL Before insert for org_id {org_id}'
                                        log_msg(SEVERITY_INFO, FILE_NAME, FUNCTION_NAME, msg)

                                        ins_sql = f'INSERT INTO {RDS_NUDGE_ANALYTICS} ' \
                                                  f'(timestamp,organization_id,message_id,nudge_type,recalc_flag,is_final_flag,day_30_analytics_flag,unique_recipients,' \
                                                  f'total_impressions,engagements,engagements_rate,' \
                                                  f'last_week_members_messaged,last_week_members_messaged_percent_trend, last_week_members_messaged_trend_arrow,' \
                                                  f'last_week_total_impressions,last_week_total_impressions_percent_trend,last_week_total_impressions_trend_arrow,last_week_engagements,' \
                                                  f'last_week_engagements_percent_trend,last_week_engagements_trend_arrow,last_week_engagements_rate_trend,' \
                                                  f'last_week_engagements_rate_percent_trend,last_week_engagements_rate_trend_arrow,' \
                                                  f'last_month_members_messaged,last_month_members_messaged_percent_trend,' \
                                                  f'last_month_members_messaged_trend_arrow,last_month_total_impressions,last_month_total_impressions_percent_trend ,' \
                                                  f'last_month_total_impressions_trend_arrow,last_month_engagements,last_month_engagements_percent_trend,' \
                                                  f'last_month_engagements_trend_arrow,last_month_engagements_rate_trend,last_month_engagements_rate_percent_trend,last_month_engagements_rate_trend_arrow,' \
                                                  f'last_90day_members_messaged,last_90day_members_messaged_percent_trend ,last_90day_members_messaged_trend_arrow ,last_90day_total_impressions,' \
                                                  f'last_90day_total_impressions_percent_trend,last_90day_total_impressions_trend_arrow ,last_90day_engagements ,' \
                                                  f'last_90day_engagements_percent_trend ,last_90day_engagements_trend_arrow,' \
                                                  f'last_90day_engagements_rate_trend,last_90day_engagements_rate_percent_trend,last_90day_engagements_rate_trend_arrow) ' \
                                                  f'VALUES ' \
                                                  f'(:timestamp,:organization_id,:message_id,:nudge_type,:recalc_flag,:is_final_flag,:day_30_analytics_flag,:unique_recipients ' \
                                                  f':total_impressions,:engagements,:engagements_rate, ' \
                                                  f':last_week_members_messaged,:last_week_members_messaged_percent_trend, :last_week_members_messaged_trend_arrow,' \
                                                  f':last_week_total_impressions,:last_week_total_impressions_percent_trend,:last_week_total_impressions_trend_arrow,:last_week_engagements,' \
                                                  f':last_week_engagements_percent_trend,:last_week_engagements_trend_arrow,' \
                                                  f':last_week_engagements_rate_trend,:last_week_engagements_rate_percent_trend,:last_week_engagements_rate_trend_arrow,' \
                                                  f':last_month_members_messaged,:last_month_members_messaged_percent_trend,' \
                                                  f':last_month_members_messaged_trend_arrow,:last_month_total_impressions,:last_month_total_impressions_percent_trend ,' \
                                                  f':last_month_total_impressions_trend_arrow,:last_month_engagements,' \
                                                  f':last_month_engagements_percent_trend,:last_month_engagements_trend_arrow,' \
                                                  f':last_month_engagements_rate_trend,:last_month_engagements_rate_percent_trend,:last_month_engagements_rate_trend_arrow,' \
                                                  f':last_90day_members_messaged,:last_90day_members_messaged_percent_trend ,:last_90day_members_messaged_trend_arrow ,:last_90day_total_impressions,' \
                                                  f':last_90day_total_impressions_percent_trend,:last_90day_total_impressions_trend_arrow ,:last_90day_engagements ,' \
                                                  f':last_90day_engagements_percent_trend ,:last_90day_engagements_trend_arrow,:last_90day_engagements_rate_trend,:last_90day_engagements_rate_percent_trend,:last_90day_engagements_rate_trend_arrow) '
                                        res = db.session.execute(ins_sql, params)
                                        db.session.commit()
                                        if res:
                                            insert_update = "INSERTED"
                                            total_inserted += 1
                                            msg = f'MYSQL Insert successful for org_id {org_id} {ins_sql} {params}'
                                            log_msg(SEVERITY_INFO, FILE_NAME, FUNCTION_NAME, msg)
                                    except Exception as err:
                                        msg = f'MYSQL INSERT EXCEPTION. INSSQL={ins_sql} Params={params} Exception={str(err)}'
                                        log_msg(SEVERITY_INFO, FILE_NAME, FUNCTION_NAME, msg)
                                        total_errors += 1
                                        # do not stop on exceptions, keep processing
                                        continue

                                if insert_update == None:
                                    total_errors += 1
                                    msg = f'MYSQL Unable to insert/update record with Params={params}'
                                    log_msg(SEVERITY_ERROR, FILE_NAME, FUNCTION_NAME, msg)
                                    total_errors += 1
                                # commit every x records
                                elif (total_updated + total_inserted) % 250 == 0:
                                    db.session.commit()

                            else:
                                try:
                                    msg = f'message_id {message_id} is not geo based nudge. Its {nudge_type}'
                                    log_msg(SEVERITY_INFO, FILE_NAME, FUNCTION_NAME, msg)

                                    res_tb = messaging_reach_query(org_id=org_id, interval_high=30, interval_low=0,
                                                                   nudge_today=0, message_id=message_id,
                                                                   nudge_type=nudge_type)[0]
                                    tb_unique_members = int(res_tb["unique_members"])
                                    tb_total_impressions = int(res_tb["totalimpressions"])
                                    tb_total_engagements = int(res_tb["totalengagements"])
                                    tb_enagement_rate = float(res_tb["engagementrate"])

                                    recalc_flag = 0
                                    is_final_flag = 0
                                    day_30_analytics_flag = 0

                                    # try to update 1st
                                    params = {"timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                                              "organization_id": org_id,
                                              "message_id": message_id, "nudge_type": nudge_type,
                                              "recalc_flag": recalc_flag,
                                              "is_final_flag": is_final_flag,
                                              "day_30_analytics_flag": day_30_analytics_flag,
                                              "unique_recipients": tb_unique_members,
                                              "total_impressions": tb_total_impressions,
                                              "engagements": tb_total_engagements,
                                              "engagements_rate": tb_enagement_rate}

                                    params_mr = {}
                                    # Check if there are any records in messaging_reach table
                                    sql_q = f"select count(*) from {RDS_NUDGE_ANALYTICS} where organization_id = :org_id " \
                                            f"and message_id = :message_id"

                                    params_mr["org_id"] = org_id
                                    params_mr["message_id"] = message_id
                                    total_results = db.session.execute(sql_q, params_mr).scalar()

                                    if total_results:
                                        insert_update = "Update"
                                    else:
                                        insert_update = "Insert"
                                    if insert_update == "Update":
                                        try:
                                            upd_sql = f'UPDATE {RDS_NUDGE_ANALYTICS} ' \
                                                      f'SET timestamp = :timestamp,recalc_flag = :recalc_flag,' \
                                                      f'is_final_flag=:is_final_flag,day_30_analytics_flag = :day_30_analytics_flag,unique_recipients = :unique_recipients,' \
                                                      f'total_impressions = :total_impressions,engagements = :engagements,' \
                                                      f'engagements_rate = :engagements_rate ' \
                                                      f'WHERE organization_id=:organization_id and message_id = :message_id and nudge_type = :nudge_type'

                                            res = db.session.execute(upd_sql, params)
                                            db.session.commit()
                                            if res.rowcount > 0:
                                                insert_update = "UPDATED"
                                                msg = f'MYSQL UPDATE successful for org_id {org_id} for message_id {message_id}'
                                                log_msg(SEVERITY_INFO, FILE_NAME, FUNCTION_NAME, msg)
                                                total_updated += 1
                                        except Exception as err:
                                            msg = f'MYSQL UPDATE EXCEPTION. UPDSQL={upd_sql} Params={params} Exception={str(err)}'
                                            log_msg(SEVERITY_INFO, FILE_NAME, FUNCTION_NAME, msg)
                                            total_errors += 1
                                            # do not stop on exceptions, keep processing
                                            continue

                                    # try to insert if update did not work or do anything
                                    elif insert_update == 'Insert':
                                        try:
                                            msg = f'MYSQL Before insert for org_id {org_id}'
                                            log_msg(SEVERITY_INFO, FILE_NAME, FUNCTION_NAME, msg)

                                            ins_sql = f'INSERT INTO {RDS_NUDGE_ANALYTICS} ' \
                                                      f'(timestamp,organization_id,message_id,nudge_type,recalc_flag,' \
                                                      f'is_final_flag,day_30_analytics_flag,unique_recipients,total_impressions,engagements,engagements_rate) ' \
                                                      f'VALUES ' \
                                                      f'(:timestamp,:organization_id,:message_id,:nudge_type,:recalc_flag,' \
                                                      f':is_final_flag,:day_30_analytics_flag,:unique_recipients,:total_impressions,:engagements,:engagements_rate) '
                                            res = db.session.execute(ins_sql, params)
                                            db.session.commit()
                                            if res:
                                                insert_update = "INSERTED"
                                                total_inserted += 1
                                                msg = f'MYSQL Insert successful for org_id {org_id} for message_id {message_id} {ins_sql} {params}'
                                                log_msg(SEVERITY_INFO, FILE_NAME, FUNCTION_NAME, msg)
                                        except Exception as err:
                                            msg = f'MYSQL INSERT EXCEPTION. INSSQL={ins_sql} Params={params} Exception={str(err)}'
                                            log_msg(SEVERITY_INFO, FILE_NAME, FUNCTION_NAME, msg)
                                            total_errors += 1
                                            # do not stop on exceptions, keep processing
                                            continue

                                    if insert_update == None:
                                        total_errors += 1
                                        msg = f'MYSQL Unable to insert/update record with Params={params}'
                                        log_msg(SEVERITY_ERROR, FILE_NAME, FUNCTION_NAME, msg)
                                        total_errors += 1
                                    # commit every x records
                                    elif (total_updated + total_inserted) % 250 == 0:
                                        db.session.commit()
                                except Exception as err:
                                    msg = f'Exception in individual org_id {org_id}'
                                    log_msg(SEVERITY_INFO, FILE_NAME, FUNCTION_NAME, msg)


                        except Exception as err:
                            exc_type, exc_obj, exc_tb = sys.exc_info()
                            msg = f'Exception in individual org_id {org_id} for message_id {message_id} {err} line number {str(exc_tb.tb_lineno)}'
                            log_msg(SEVERITY_INFO, FILE_NAME, FUNCTION_NAME, msg)
                            total_errors += 1
                            # do not stop on exceptions, keep processing
                            continue

                        num_orgs += 1

                    timer.stop()
                    msg = f" Calculated nudge analytics for {num_orgs} messages."
                    log_msg(SEVERITY_INFO, FILE_NAME, FUNCTION_NAME, msg)

                    publish_audit_event(AuditEvents.AUDIT_EVENT_NUDGE_ANALYTICS,
                                        AuditOps.AUDIT_OP_STOP,
                                        total_errors,
                                        additional_msg=msg,
                                        elapsed_secs=timer.elapsed_time())

                    return 201

                except Exception as e:
                    timer.stop()
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    msg = f"TASK_ID:{task_id} calculating nudge analytics query  EXCEPTION  Ex: {str(e)} for message_id {message_id} line number {str(exc_tb.tb_lineno)}"
                    log_msg(SEVERITY_INFO, FILE_NAME, FUNCTION_NAME, msg)
                    return msg

    except Exception as e:
        timer.stop()
        msg = f"TASK_ID:{task_id} calculating nudge analytics query  EXCEPTION  Ex: {str(e)} for message_id {message_id}"
        log_msg(SEVERITY_ERROR, FILE_NAME, FUNCTION_NAME, msg)
        raise e


@celery_app.task(bind=True, acks_late=True)
def calc_nudge_today_async_delay(self):
    """
        Function to caluclate dynamic segments asynchronously
        :return: 201
    """
    from core.views.analytics_dashboard import messaging_reach_query
    FUNCTION_NAME = "calc_nudge_today_async_delay"
    num_orgs = 0
    errors = 0
    timer = Timer()
    timer.start()
    try:
        from core.app import create_app
        with create_app().app_context():
            with app.test_request_context('/analytics-dashboard/async_nudge_today'):
                try:
                    task_id = self.request.id
                    msg = f"TASK_ID:{task_id} Start calculating nudge today"
                    log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)

                    # update the timestamp to today's for demo orgs in nudge_today calculations
                    sql_str = (f'select o.id as id from organization o inner join organization_type ot '
                               f'on o.organization_type_id = ot.id where o.active = 1 and ot.calculate_analytics = 0 order by id desc')
                    # Run the SQL
                    result = db.session.execute(sql_str)
                    for id in result:
                        try:
                            no_calc_org_id = id[0]
                            params_q = {"org_id": no_calc_org_id}
                            count_query = f"SELECT count(*) FROM analytics_insights_by_org_nudge_today where organization_id = :org_id"
                            total_results = db.session.execute(count_query, params_q).scalar()

                            msg = f'count nudge today demo org_id {total_results}'
                            log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)

                            if total_results > 0:
                                sql_q = f"update analytics_insights_by_org_nudge_today set timestamp = current_timestamp() " \
                                        f"where organization_id = :org_id"
                                res = db.session.execute(sql_q, params_q)
                                db.session.commit()
                                msg = f'inside update {res}'
                                log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)
                        except Exception as err:
                            timer.stop()
                            errors += 1
                            msg = f"GENERAL EXCEPTION: TASK_ID:{task_id} updating timestamp for demo orgs nudge today for org_id {no_calc_org_id}  x: {str(err)}"
                            log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)

                    # calculate nudge today analytics for all active orgs where flag for calculate_analytics is turned on
                    sql_str = (f'select o.id as id from organization o inner join organization_type ot '
                               f'on o.organization_type_id = ot.id where o.active = 1 and ot.calculate_analytics = 1 order by id desc')
                    # Run the SQL
                    result = db.session.execute(sql_str)
                    for id in result:
                        try:
                            val_list = []
                            org_id = id[0]
                            msg = f"TASK_ID:{task_id} Start calculating nudge today for org_id {org_id}"
                            log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)

                            res_perday = \
                                messaging_reach_query(org_id=org_id, interval_high=0, interval_low=0, nudge_today=1)[0]
                            if res_perday["unique_members"] > 0:
                                total_members = res_perday["unique_members"]
                                msg = f"TASK_ID:{task_id} appending nudge today {total_members} for org_id {org_id}"
                                log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)

                                title = INSIGHT_STATIC_CARD_NUDGE_TODAY_TITLE
                                Members_Messaged = int(res_perday["unique_members"])
                                Total_Impressions = int(res_perday["totalimpressions"])
                                Engagements = int(res_perday["totalengagements"])
                                description = ''
                                icon = INSIGHT_ICON_NUDGE_TODAY
                                url = ''
                                ranking = 1
                                val_tuple = (
                                    datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'), org_id, title, Members_Messaged,
                                    Total_Impressions,
                                    Engagements, description, icon, url, ranking)
                                val_list.append(val_tuple)
                                msg = f"TASK_ID:{task_id} appending nudge today {val_list} for org_id {org_id}"
                                log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)

                                # Get the record count
                                count_query = f"SELECT count(*) FROM analytics_insights_by_org_nudge_today where organization_id = :org_id"
                                params_q = {"org_id": org_id}
                                total_results = db.session.execute(count_query, params_q).scalar()

                                msg = f'count insights org_id {total_results}'
                                log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)

                                if total_results > 0:
                                    params_q = {}
                                    sql_q = f"delete from analytics_insights_by_org_nudge_today where organization_id = :org_id"
                                    params_q = {"org_id": org_id}
                                    res = db.session.execute(sql_q, params_q)
                                    db.session.commit()
                                    msg = f'inside delete {res}'
                                    log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)

                                    msg = f'Before MYSQL Insert analytics_insights_by_org_nudge_today org_id {org_id} in {(val_list)}'
                                    log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)

                                if len(val_list) > 0:
                                    values = ', '.join(map(str, val_list))
                                    sql = "INSERT INTO analytics_insights_by_org_nudge_today (timestamp,organization_id,title,Members_Messaged," \
                                          "Total_Impressions,Engagements,description,icon,url,ranking) VALUES {}".format(
                                        values)
                                    msg = f'MYSQL Insert analytics_insights_by_org_nudge_today org_id {org_id} in {sql}'
                                    log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)
                                    db.session.execute(sql)
                                    db.session.commit()
                                    msg = f'MYSQL Insert successful for analytics_insights_by_org_nudge_today org_id {org_id} '
                                    log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)

                        except Exception as err:
                            timer.stop()
                            errors += 1
                            msg = f"GENERAL EXCEPTION: TASK_ID:{task_id} calculating nudge today for org_id {org_id}  EXCEPTION  Ex: {str(err)}"
                            log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)
                        num_orgs += 1
                except Exception as e:
                    timer.stop()
                    errors += 1
                    msg = f"TASK_ID:{task_id} calculating nudge today for org_id {org_id}  EXCEPTION  Ex: {str(e)}"
                    log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)
                    return msg

                timer.stop()
                # msg = f"TASK_ID:{task_id} Finished calculating nudge today for all orgs in {timer.elapsed_time_str()}"
                msg = f" Calculated {num_orgs} orgs for nudge today."
                log_msg(SEVERITY_DEBUG, FILE_NAME, FUNCTION_NAME, msg)

                publish_audit_event(AuditEvents.AUDIT_EVENT_NUDGE_TODAY_TASK,
                                    AuditOps.AUDIT_OP_STOP,
                                    errors,
                                    additional_msg=msg,
                                    elapsed_secs=timer.elapsed_time())

                return 201

    except Exception as err:
        timer.stop()
        msg = f"GENERAL EXCEPTION: TASK_ID:{task_id} calculating nudge today for org_id {org_id}  EXCEPTION  Ex: {str(err)}"
        log_msg(SEVERITY_ERROR, FILE_NAME, FUNCTION_NAME, msg)
        raise err
