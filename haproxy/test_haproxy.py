# (C) Datadog, Inc. 2012-2016
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

# stdlib
from collections import defaultdict
import copy
import os

# 3p
import mock
from nose.plugins.attrib import attr
from unittest.case import SkipTest

# project
from checks import AgentCheck
from tests.checks.common import AgentCheckTest
from utils.hostname import get_hostname
from utils.platform import Platform

MOCK_DATA = """# pxname,svname,qcur,qmax,scur,smax,slim,stot,bin,bout,dreq,dresp,ereq,econ,eresp,wretr,wredis,status,weight,act,bck,chkfail,chkdown,lastchg,downtime,qlimit,pid,iid,sid,throttle,lbtot,tracked,type,rate,rate_lim,rate_max,check_status,check_code,check_duration,hrsp_1xx,hrsp_2xx,hrsp_3xx,hrsp_4xx,hrsp_5xx,hrsp_other,hanafail,req_rate,req_rate_max,req_tot,cli_abrt,srv_abrt,
a,FRONTEND,,,1,2,12,1,11,11,0,0,0,,,,,OPEN,,,,,,,,,1,1,0,,,,0,1,0,2,,,,0,1,0,0,0,0,,1,1,1,,,
a,BACKEND,0,0,0,0,12,0,11,11,0,0,,0,0,0,0,UP,0,0,0,,0,1221810,0,,1,1,0,,0,,1,0,,0,,,,0,0,0,0,0,0,,,,,0,0,
b,FRONTEND,,,1,2,12,11,11,0,0,0,0,,,,,OPEN,,,,,,,,,1,2,0,,,,0,0,0,1,,,,,,,,,,,0,0,0,,,
b,i-1,0,0,0,1,,1,1,0,,0,,0,0,0,0,UP 1/2,1,1,0,0,1,1,30,,1,3,1,,70,,2,0,,1,1,,0,,,,,,,0,,,,0,0,
b,i-2,0,0,1,1,,1,1,0,,0,,0,0,0,0,UP 1/2,1,1,0,0,0,1,0,,1,3,2,,71,,2,0,,1,1,,0,,,,,,,0,,,,0,0,
b,i-3,0,0,0,1,,1,1,0,,0,,0,0,0,0,UP,1,1,0,0,0,1,0,,1,3,3,,70,,2,0,,1,1,,0,,,,,,,0,,,,0,0,
b,i-4,0,0,0,1,,1,1,0,,0,,0,0,0,0,DOWN,1,1,0,0,0,1,0,,1,3,3,,70,,2,0,,1,1,,0,,,,,,,0,,,,0,0,
b,i-5,0,0,0,1,,1,1,0,,0,,0,0,0,0,MAINT,1,1,0,0,0,1,0,,1,3,3,,70,,2,0,,1,1,,0,,,,,,,0,,,,0,0,
b,BACKEND,0,0,1,2,0,421,1,0,0,0,,0,0,0,0,UP,6,6,0,,0,1,0,,1,3,0,,421,,1,0,,1,,,,,"space, afterspace",,,,,,,,,0,0,
c,i-1,0,0,0,1,,1,1,0,,0,,0,0,0,0,UP,1,1,0,0,1,1,30,,1,3,1,,70,,2,0,,1,1,,0,,,,,,,0,,,,0,0,
c,i-2,0,0,0,1,,1,1,0,,0,,0,0,0,0,DOWN (agent),1,1,0,0,1,1,30,,1,3,1,,70,,2,0,,1,1,,0,,,,,,,0,,,,0,0,
c,i-3,0,0,0,1,,1,1,0,,0,,0,0,0,0,NO CHECK,1,1,0,0,1,1,30,,1,3,1,,70,,2,0,,1,1,,0,,,,,,,0,,,,0,0,
c,BACKEND,0,0,1,2,0,421,1,0,0,0,,0,0,0,0,UP,6,6,0,,0,1,0,,1,3,0,,421,,1,0,,1,,,,,,,,,,,,,,0,0,
"""

MOCK_DATA_2 = """# pxname,svname,qcur,qmax,scur,smax,slim,stot,bin,bout,dreq,dresp,ereq,econ,eresp,wretr,wredis,status,weight,act,bck,chkfail,chkdown,lastchg,downtime,qlimit,pid,iid,sid,throttle,lbtot,tracked,type,rate,rate_lim,rate_max,check_status,check_code,check_duration,hrsp_1xx,hrsp_2xx,hrsp_3xx,hrsp_4xx,hrsp_5xx,hrsp_other,hanafail,req_rate,req_rate_max,req_tot,cli_abrt,srv_abrt,comp_in,comp_out,comp_byp,comp_rsp,lastsess,last_chk,last_agt,qtime,ctime,rtime,ttime,agent_status,agent_code,agent_duration,check_desc,agent_desc,check_rise,check_fall,check_health,agent_rise,agent_fall,agent_health,addr,cookie,mode,algo,conn_rate,conn_rate_max,conn_tot,intercepted,dcon,dses,
stats,FRONTEND,,,1,4,2000,70959,21126224,562247026,0,0,0,,,,,OPEN,,,,,,,,,1,1,0,,,,0,1,0,4,,,,0,70957,1,0,0,0,,1,3,70959,,,0,0,0,0,,,,,,,,,,,,,,,,,,,,,http,,1,4,70959,70959,0,0,
stats,BACKEND,0,0,0,0,200,0,21126224,562247026,0,0,,0,0,0,0,UP,0,0,0,,0,1350229,,,1,1,0,,0,,1,0,,0,,,,0,0,0,0,0,0,,,,0,0,0,0,0,0,0,0,,,0,0,0,0,,,,,,,,,,,,,,http,,,,,,,,
lexcorp,gc-web-live-a19b,0,0,0,101,,2743690,11982154362,55181827051,,0,,0,430,6,0,UP,100,1,0,68,8,138761,868,,1,2,1,,2743684,,2,0,,84,L7OK,200,6,1448,2643369,54783,38180,5832,0,0,,,,726,359,,,,,2,HTTP status check returned code <200>,"agent warns : Backend is using a static LB algorithm and only accepts weights '0%' and '100%'.
",0,0,423,657,CHECKED,,1,Layer7 check passed,No status change,2,3,4,1,1,1,,,http,,,,,,,,
lexcorp,gc-web-live-gvbv,0,0,4,107,,3861585,14308710973,74521653047,,0,,0,444,8,0,UP,100,1,0,89,11,138915,1128,,1,2,2,,3861577,,2,2,,87,L7OK,200,4,1747,3739849,73523,40150,6209,0,0,,,,871,348,,,,,0,HTTP status check returned code <200>,"agent warns : Backend is using a static LB algorithm and only accepts weights '0%' and '100%'.
",0,0,55,155,CHECKED,,0,Layer7 check passed,No status change,2,3,4,1,1,1,,,http,,,,,,,,
lexcorp,gc-web-live-z5b9,0,0,0,120,,3078252,11421437866,49586684204,,0,,0,505,6,0,UP,100,1,0,57,9,138457,1428,,1,2,3,,3078246,,2,0,,77,L7OK,200,4,1619,2963216,64386,43450,5478,0,0,,,,649,408,,,,,2,HTTP status check returned code <200>,"agent warns : Backend is using a static LB algorithm and only accepts weights '0%' and '100%'.
",0,0,132,166,CHECKED,,0,Layer7 check passed,No status change,2,3,4,1,1,1,,,http,,,,,,,,
lexcorp,BACKEND,0,0,4,125,200,9683507,37712303201,179290164302,0,0,,0,1379,20,0,UP,300,3,0,,0,1350229,0,,1,2,0,,9683507,,1,2,,96,,,,4810,9346434,192692,121780,17783,4,,,,9683503,2246,1115,0,0,0,0,0,,,0,0,225,381,,,,,,,,,,,,,,http,,,,,,,,
lexcorpscheduler,gc-scheduler-web-live-5x7b,0,0,0,2,,5169,13699188,9387327,,0,,0,0,0,0,UP,100,1,0,439,21,138102,1051,,1,3,1,,5169,,2,0,,3,L7OK,200,15,0,5160,0,9,0,0,0,,,,0,0,,,,,84438,HTTP status check returned code <200>,,0,0,28,28,CHECKED,,0,Layer7 check passed,No status change,2,3,4,1,1,1,,,http,,,,,,,,
lexcorpscheduler,gc-scheduler-web-live-7hla,0,0,0,2,,5173,13714694,8613749,,0,,0,0,0,0,UP,100,1,0,479,15,138299,974,,1,3,2,,5173,,2,0,,3,L7OK,200,20,0,5162,0,11,0,0,0,,,,0,0,,,,,84436,HTTP status check returned code <200>,,0,0,49,49,CHECKED,,7,Layer7 check passed,No status change,2,3,4,1,1,1,,,http,,,,,,,,
lexcorpscheduler,BACKEND,0,0,0,4,200,10342,27413882,18001076,0,0,,0,0,0,0,UP,200,2,0,,1,151702,6,,1,3,0,,10342,,1,0,,5,,,,0,10322,0,20,0,0,,,,10342,0,0,0,0,0,0,84436,,,0,0,34,34,,,,,,,,,,,,,,http,,,,,,,,
lexcorppublisher,gc-publisher-web-live-p05r,0,0,0,19,,74902,31741301,9662017532,,0,,0,0,0,0,UP,100,1,0,69,9,139204,611,,1,4,1,,74902,,2,0,,17,L7OK,200,6,0,69241,3176,1055,1430,0,0,,,,9,0,,,,,313,HTTP status check returned code <200>,,0,0,2296,2333,CHECKED,,0,Layer7 check passed,No status change,2,3,4,1,1,1,,,http,,,,,,,,
lexcorppublisher,gc-publisher-web-live-4z3f,0,0,0,20,,78806,33411763,10292079109,,0,,0,0,0,0,UP,96,1,0,47,8,139351,594,,1,4,2,,78806,,2,0,,16,L7OK,200,5,0,72864,3292,1192,1458,0,0,,,,8,0,,,,,313,HTTP status check returned code <200>,,0,0,737,745,CHECKED,,0,Layer7 check passed,No status change,2,3,4,1,1,1,,,http,,,,,,,,
lexcorppublisher,gc-publisher-web-live-61h2,0,0,0,12,,79374,33692433,10320053962,,0,,0,0,0,0,UP,99,1,0,47,8,137789,604,,1,4,3,,79374,,2,0,,17,L7OK,200,4,0,73020,3491,1220,1643,0,0,,,,5,0,,,,,27,HTTP status check returned code <200>,,0,0,784,792,CHECKED,,0,Layer7 check passed,No status change,2,3,4,1,1,1,,,http,,,,,,,,
lexcorppublisher,BACKEND,0,0,0,30,200,233082,98845497,30274150603,0,0,,0,0,0,0,UP,295,3,0,,0,1350229,0,,1,4,0,,233082,,1,0,,50,,,,0,215125,9959,3467,4531,0,,,,233082,22,0,0,0,0,0,27,,,0,0,1424,1443,,,,,,,,,,,,,,http,,,,,,,,
lexcorpstatic,gc-web-live-a19b,0,0,0,9,,289662,191623353,16176598861,,0,,0,0,0,2,UP,76,1,0,78,8,138761,865,,1,5,1,,289662,,2,0,,16,L7OK,200,4,0,216013,6702,66380,565,0,0,,,,11,0,,,,,5,HTTP status check returned code <200>,,0,0,16,18,CHECKED,,1,Layer7 check passed,No status change,2,3,4,1,1,1,,,http,,,,,,,,
lexcorpstatic,gc-web-live-gvbv,0,0,0,10,,266780,176238604,14985496143,,0,,0,0,0,0,UP,97,1,0,85,12,138915,1141,,1,5,2,,266780,,2,0,,14,L7OK,200,10,0,197049,6279,63129,323,0,0,,,,18,0,,,,,4,HTTP status check returned code <200>,,0,0,12,14,CHECKED,,0,Layer7 check passed,No status change,2,3,4,1,1,1,,,http,,,,,,,,
lexcorpstatic,gc-web-live-z5b9,0,0,0,11,,288180,190539746,15981693605,,0,,0,1,0,0,UP,98,1,0,61,9,138457,1429,,1,5,3,,288180,,2,1,,17,L7OK,200,8,0,214572,6747,66488,372,0,0,,,,11,0,,,,,1,HTTP status check returned code <200>,,0,0,10,12,CHECKED,,0,Layer7 check passed,No status change,2,3,4,1,1,1,,,http,,,,,,,,
lexcorpstatic,BACKEND,0,0,0,16,200,844620,558401703,47143788609,0,0,,0,1,0,2,UP,271,3,0,,0,1350229,0,,1,5,0,,844622,,1,1,,36,,,,0,627634,19728,195997,1261,0,,,,844620,40,0,0,0,0,0,1,,,0,0,14,15,,,,,,,,,,,,,,http,,,,,,,,
websitemanager,gc-certmgr-live-1,0,0,0,3,,75646,36425153,44718523,,0,,0,1,0,3,UP,100,1,0,80,15,67221,1215,,1,6,1,,75646,,2,0,,9,L7OK,200,59,0,75466,152,20,4,0,0,,,,0,0,,,,,2,HTTP check did not match unwanted content,,0,0,39,40,,,,Layer7 check passed,,2,3,4,,,,,,http,,,,,,,,
websitemanager,gc-certmgr-live-2,0,0,0,5,,75770,36486003,49347964,,0,,0,0,0,1,UP,100,1,0,57,12,67220,674,,1,6,2,,75770,,2,1,,14,L7OK,200,67,0,75597,153,16,3,0,0,,,,4,0,,,,,1,HTTP check did not match unwanted content,,0,0,51,52,,,,Layer7 check passed,,2,3,4,,,,,,http,,,,,,,,
websitemanager,gc-certmgr-live-3,0,0,0,6,,75719,36336359,47971418,,0,,0,1,0,11,UP,100,1,0,50,12,67221,739,,1,6,3,,75719,,2,1,,9,L7OK,200,37,0,75560,126,15,6,0,0,,,,0,0,,,,,1,HTTP check did not match unwanted content,,0,0,46,47,,,,Layer7 check passed,,2,3,4,,,,,,http,,,,,,,,
websitemanager,BACKEND,0,0,0,9,200,227177,109274642,142049761,0,0,,57,2,0,15,UP,300,3,0,,3,67221,405,,1,6,0,,227135,,1,0,,27,,,,0,226623,431,51,72,0,,,,227177,4,0,0,0,0,0,1,,,0,0,46,46,,,,,,,,,,,,,,http,,,,,,,,
lexcorp,FRONTEND,,,4,125,2000,9683858,37712306031,179290228535,0,0,351,,,,,OPEN,,,,,,,,,1,7,0,,,,0,2,0,96,,,,4810,9346434,192692,122131,17783,4,,2,96,9683858,,,0,0,0,0,,,,,,,,,,,,,,,,,,,,,http,,2,96,9683858,0,0,0,
lexcorpscheduler,FRONTEND,,,0,4,2000,10342,27413882,18001076,0,0,0,,,,,OPEN,,,,,,,,,1,8,0,,,,0,0,0,5,,,,0,10322,0,20,0,0,,0,5,10342,,,0,0,0,0,,,,,,,,,,,,,,,,,,,,,http,,0,5,10342,0,0,0,
lexcorppublisher,FRONTEND,,,0,30,2000,233082,98845497,30274150603,0,0,0,,,,,OPEN,,,,,,,,,1,9,0,,,,0,0,0,50,,,,0,215125,9959,3467,4531,0,,0,50,233082,,,0,0,0,0,,,,,,,,,,,,,,,,,,,,,http,,0,50,233082,0,0,0,
lexcorpstatic,FRONTEND,,,0,16,2000,844620,558401703,47143788609,0,0,0,,,,,OPEN,,,,,,,,,1,10,0,,,,0,1,0,36,,,,0,627634,19728,195997,1261,0,,1,36,844620,,,0,0,0,0,,,,,,,,,,,,,,,,,,,,,http,,1,36,844620,0,0,0,
websitemanager,FRONTEND,,,0,9,2000,227177,109274642,142049761,0,0,0,,,,,OPEN,,,,,,,,,1,11,0,,,,0,0,0,27,,,,0,226623,431,51,72,0,,0,27,227177,,,0,0,0,0,,,,,,,,,,,,,,,,,,,,,http,,0,27,227177,0,0,0,
"""

AGG_STATUSES_BY_SERVICE = (
    (['status:available', 'service:a'], 1),
    (['status:available', 'service:b'], 4),
    (['status:unavailable', 'service:b'], 2),
    (['status:available', 'service:c'], 1),
    (['status:unavailable', 'service:c'], 2)
)

AGG_STATUSES = (
    (['status:available'], 6),
    (['status:unavailable'], 4)
)

@attr(requires='haproxy')
class TestCheckHAProxy(AgentCheckTest):
    CHECK_NAME = 'haproxy'

    BASE_CONFIG = {
        'init_config': None,
        'instances': [
            {
                'url': 'http://localhost/admin?stats',
                'collect_status_metrics': True,
            }
        ]
    }

    def _assert_agg_statuses(self, count_status_by_service=True, collate_status_tags_per_host=False):
        expected_statuses = AGG_STATUSES_BY_SERVICE if count_status_by_service else AGG_STATUSES
        for tags, value in expected_statuses:
            if collate_status_tags_per_host:
                # Assert that no aggregate statuses are sent
                self.assertMetric('haproxy.count_per_status', tags=tags, count=0)
            else:
                self.assertMetric('haproxy.count_per_status', value=value, tags=tags)

    @mock.patch('requests.get', return_value=mock.Mock(content=MOCK_DATA))
    def test_count_per_status_agg_only(self, mock_requests):
        config = copy.deepcopy(self.BASE_CONFIG)
        # with count_status_by_service set to False
        config['instances'][0]['count_status_by_service'] = False
        self.run_check(config)

        self.assertMetric('haproxy.count_per_status', value=2, tags=['status:open'])
        self.assertMetric('haproxy.count_per_status', value=4, tags=['status:up'])
        self.assertMetric('haproxy.count_per_status', value=2, tags=['status:down'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['status:maint'])
        self.assertMetric('haproxy.count_per_status', value=0, tags=['status:nolb'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['status:no_check'])

        self._assert_agg_statuses(count_status_by_service=False)

    @mock.patch('requests.get', return_value=mock.Mock(content=MOCK_DATA))
    def test_count_per_status_by_service(self, mock_requests):
        self.run_check(self.BASE_CONFIG)

        self.assertMetric('haproxy.count_per_status', value=1, tags=['status:open', 'service:a'])
        self.assertMetric('haproxy.count_per_status', value=3, tags=['status:up', 'service:b'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['status:open', 'service:b'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['status:down', 'service:b'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['status:maint', 'service:b'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['status:up', 'service:c'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['status:down', 'service:c'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['status:no_check', 'service:c'])

        self._assert_agg_statuses()

    @mock.patch('requests.get', return_value=mock.Mock(content=MOCK_DATA))
    def test_count_per_status_by_service_and_host(self, mock_requests):
        config = copy.deepcopy(self.BASE_CONFIG)
        config['instances'][0]['collect_status_metrics_by_host'] = True
        self.run_check(config)

        self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:FRONTEND', 'status:open', 'service:a'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:FRONTEND', 'status:open', 'service:b'])
        for backend in ['i-1', 'i-2', 'i-3']:
            self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:%s' % backend, 'status:up', 'service:b'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:i-4', 'status:down', 'service:b'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:i-5', 'status:maint', 'service:b'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:i-1', 'status:up', 'service:c'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:i-2', 'status:down', 'service:c'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:i-3', 'status:no_check', 'service:c'])

        self._assert_agg_statuses()

    @mock.patch('requests.get', return_value=mock.Mock(content=MOCK_DATA))
    def test_count_per_status_by_service_and_collate_per_host(self, mock_requests):
        config = copy.deepcopy(self.BASE_CONFIG)
        config['instances'][0]['collect_status_metrics_by_host'] = True
        config['instances'][0]['collate_status_tags_per_host'] = True
        self.run_check(config)

        self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:FRONTEND', 'status:available', 'service:a'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:FRONTEND', 'status:available', 'service:b'])
        for backend in ['i-1', 'i-2', 'i-3']:
            self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:%s' % backend, 'status:available', 'service:b'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:i-4', 'status:unavailable', 'service:b'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:i-5', 'status:unavailable', 'service:b'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:i-1', 'status:available', 'service:c'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:i-2', 'status:unavailable', 'service:c'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:i-3', 'status:unavailable', 'service:c'])

        self._assert_agg_statuses(collate_status_tags_per_host=True)

    @mock.patch('requests.get', return_value=mock.Mock(content=MOCK_DATA_2))
    def test_count_per_status_by_service_and_collate_per_host_large(self, mock_requests):
        config = copy.deepcopy(self.BASE_CONFIG)
        config['instances'][0]['collect_status_metrics_by_host'] = True
        config['instances'][0]['collate_status_tags_per_host'] = True
        self.run_check(config)

        self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:FRONTEND', 'status:available', 'service:a'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:FRONTEND', 'status:available', 'service:b'])
        for backend in ['i-1', 'i-2', 'i-3']:
            self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:%s' % backend, 'status:available', 'service:b'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:i-4', 'status:unavailable', 'service:b'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:i-5', 'status:unavailable', 'service:b'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:i-1', 'status:available', 'service:c'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:i-2', 'status:unavailable', 'service:c'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:i-3', 'status:unavailable', 'service:c'])

        self._assert_agg_statuses(collate_status_tags_per_host=True)

    @mock.patch('requests.get', return_value=mock.Mock(content=MOCK_DATA))
    def test_count_per_status_collate_per_host(self, mock_requests):
        config = copy.deepcopy(self.BASE_CONFIG)
        config['instances'][0]['collect_status_metrics_by_host'] = True
        config['instances'][0]['collate_status_tags_per_host'] = True
        config['instances'][0]['count_status_by_service'] = False
        self.run_check(config)

        self.assertMetric('haproxy.count_per_status', value=2, tags=['backend:FRONTEND', 'status:available'])
        self.assertMetric('haproxy.count_per_status', value=2, tags=['backend:i-1', 'status:available'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:i-2', 'status:available'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:i-2', 'status:unavailable'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:i-3', 'status:available'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:i-3', 'status:unavailable'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:i-4', 'status:unavailable'])
        self.assertMetric('haproxy.count_per_status', value=1, tags=['backend:i-5', 'status:unavailable'])

        self._assert_agg_statuses(count_status_by_service=False, collate_status_tags_per_host=True)

    # This mock is only useful to make the first `run_check` run w/o errors (which in turn is useful only to initialize the check)
    @mock.patch('requests.get', return_value=mock.Mock(content=MOCK_DATA))
    def test_count_hosts_statuses(self, mock_requests):
        self.run_check(self.BASE_CONFIG)

        data = """# pxname,svname,qcur,qmax,scur,smax,slim,stot,bin,bout,dreq,dresp,ereq,econ,eresp,wretr,wredis,status,weight,act,bck,chkfail,chkdown,lastchg,downtime,qlimit,pid,iid,sid,throttle,lbtot,tracked,type,rate,rate_lim,rate_max,check_status,check_code,check_duration,hrsp_1xx,hrsp_2xx,hrsp_3xx,hrsp_4xx,hrsp_5xx,hrsp_other,hanafail,req_rate,req_rate_max,req_tot,cli_abrt,srv_abrt,
a,FRONTEND,,,1,2,12,1,11,11,0,0,0,,,,,OPEN,,,,,,,,,1,1,0,,,,0,1,0,2,,,,0,1,0,0,0,0,,1,1,1,,,
a,BACKEND,0,0,0,0,12,0,11,11,0,0,,0,0,0,0,UP,0,0,0,,0,1221810,0,,1,1,0,,0,,1,0,,0,,,,0,0,0,0,0,0,,,,,0,0,
b,FRONTEND,,,1,2,12,11,11,0,0,0,0,,,,,OPEN,,,,,,,,,1,2,0,,,,0,0,0,1,,,,,,,,,,,0,0,0,,,
b,i-1,0,0,0,1,,1,1,0,,0,,0,0,0,0,UP 1/2,1,1,0,0,1,1,30,,1,3,1,,70,,2,0,,1,1,,0,,,,,,,0,,,,0,0,
b,i-2,0,0,1,1,,1,1,0,,0,,0,0,0,0,UP 1/2,1,1,0,0,0,1,0,,1,3,2,,71,,2,0,,1,1,,0,,,,,,,0,,,,0,0,
b,i-3,0,0,0,1,,1,1,0,,0,,0,0,0,0,UP,1,1,0,0,0,1,0,,1,3,3,,70,,2,0,,1,1,,0,,,,,,,0,,,,0,0,
b,i-4,0,0,0,1,,1,1,0,,0,,0,0,0,0,DOWN,1,1,0,0,0,1,0,,1,3,3,,70,,2,0,,1,1,,0,,,,,,,0,,,,0,0,
b,i-5,0,0,0,1,,1,1,0,,0,,0,0,0,0,MAINT,1,1,0,0,0,1,0,,1,3,3,,70,,2,0,,1,1,,0,,,,,,,0,,,,0,0,
b,BACKEND,0,0,1,2,0,421,1,0,0,0,,0,0,0,0,UP,6,6,0,,0,1,0,,1,3,0,,421,,1,0,,1,,,,,,,,,,,,,,0,0,
""".split('\n')

        # per service
        self.check._process_data(data, True, False, collect_status_metrics=True,
                                 collect_status_metrics_by_host=False)

        expected_hosts_statuses = defaultdict(int)
        expected_hosts_statuses[('b', 'open')] = 1
        expected_hosts_statuses[('b', 'up')] = 3
        expected_hosts_statuses[('b', 'down')] = 1
        expected_hosts_statuses[('b', 'maint')] = 1
        expected_hosts_statuses[('a', 'open')] = 1
        self.assertEquals(self.check.hosts_statuses, expected_hosts_statuses)

        # backend hosts
        agg_statuses = self.check._process_backend_hosts_metric(expected_hosts_statuses)
        expected_agg_statuses = {
            'a': {'available': 0, 'unavailable': 0},
            'b': {'available': 3, 'unavailable': 2},
        }
        self.assertEquals(expected_agg_statuses, dict(agg_statuses))

        # with process_events set to True
        self.check._process_data(data, True, True, collect_status_metrics=True,
                                 collect_status_metrics_by_host=False)
        self.assertEquals(self.check.hosts_statuses, expected_hosts_statuses)

        # per host
        self.check._process_data(data, True, False, collect_status_metrics=True,
                                 collect_status_metrics_by_host=True)
        expected_hosts_statuses = defaultdict(int)
        expected_hosts_statuses[('b', 'FRONTEND', 'open')] = 1
        expected_hosts_statuses[('a', 'FRONTEND', 'open')] = 1
        expected_hosts_statuses[('b', 'i-1', 'up')] = 1
        expected_hosts_statuses[('b', 'i-2', 'up')] = 1
        expected_hosts_statuses[('b', 'i-3', 'up')] = 1
        expected_hosts_statuses[('b', 'i-4', 'down')] = 1
        expected_hosts_statuses[('b', 'i-5', 'maint')] = 1
        self.assertEquals(self.check.hosts_statuses, expected_hosts_statuses)

        self.check._process_data(data, True, True, collect_status_metrics=True,
                                 collect_status_metrics_by_host=True)
        self.assertEquals(self.check.hosts_statuses, expected_hosts_statuses)

    @mock.patch('requests.get', return_value=mock.Mock(content=MOCK_DATA))
    def test_optional_tags(self, mock_requests):
        config = copy.deepcopy(self.BASE_CONFIG)
        config['instances'][0]['tags'] = ['new-tag', 'my:new:tag']

        self.run_check(config)

        self.assertMetricTag('haproxy.backend.session.current', 'new-tag')
        self.assertMetricTag('haproxy.backend.session.current', 'my:new:tag')
        self.assertMetricTag('haproxy.count_per_status', 'my:new:tag')
        self.assertServiceCheck('haproxy.backend_up', tags=['service:a', 'new-tag', 'my:new:tag', 'backend:BACKEND'])


@attr(requires='haproxy')
class HaproxyTest(AgentCheckTest):
    CHECK_NAME = 'haproxy'

    BACKEND_SERVICES = ['anotherbackend', 'datadog']

    BACKEND_LIST = ['singleton:8080', 'singleton:8081', 'otherserver']

    FRONTEND_CHECK_GAUGES = [
        'haproxy.frontend.session.current',
        'haproxy.frontend.session.limit',
        'haproxy.frontend.session.pct',
    ]

    FRONTEND_CHECK_GAUGES_POST_1_4 = [
        'haproxy.frontend.requests.rate',
    ]

    BACKEND_CHECK_GAUGES = [
        'haproxy.backend.queue.current',
        'haproxy.backend.session.current',
    ]

    BACKEND_CHECK_GAUGES_POST_1_5 = [
        'haproxy.backend.queue.time',
        'haproxy.backend.connect.time',
        'haproxy.backend.response.time',
        'haproxy.backend.session.time',
    ]

    FRONTEND_CHECK_RATES = [
        'haproxy.frontend.bytes.in_rate',
        'haproxy.frontend.bytes.out_rate',
        'haproxy.frontend.denied.req_rate',
        'haproxy.frontend.denied.resp_rate',
        'haproxy.frontend.errors.req_rate',
        'haproxy.frontend.session.rate',
    ]

    FRONTEND_CHECK_RATES_POST_1_4 = [
        'haproxy.frontend.response.1xx',
        'haproxy.frontend.response.2xx',
        'haproxy.frontend.response.3xx',
        'haproxy.frontend.response.4xx',
        'haproxy.frontend.response.5xx',
        'haproxy.frontend.response.other',
    ]

    BACKEND_CHECK_RATES = [
        'haproxy.backend.bytes.in_rate',
        'haproxy.backend.bytes.out_rate',
        'haproxy.backend.denied.resp_rate',
        'haproxy.backend.errors.con_rate',
        'haproxy.backend.errors.resp_rate',
        'haproxy.backend.session.rate',
        'haproxy.backend.warnings.redis_rate',
        'haproxy.backend.warnings.retr_rate',
    ]

    BACKEND_CHECK_RATES_POST_1_4 = [
        'haproxy.backend.response.1xx',
        'haproxy.backend.response.2xx',
        'haproxy.backend.response.3xx',
        'haproxy.backend.response.4xx',
        'haproxy.backend.response.5xx',
        'haproxy.backend.response.other',
    ]

    def __init__(self, *args, **kwargs):
        AgentCheckTest.__init__(self, *args, **kwargs)
        self.config = {
            "instances": [{
                'url': 'http://localhost:3835/stats',
                'username': 'datadog',
                'password': 'isdevops',
                'status_check': True,
                'collect_aggregates_only': False,
                'tag_service_check_by_host': True,
            }]
        }
        self.config_open = {
            'instances': [{
                'url': 'http://localhost:3836/stats',
                'collect_aggregates_only': False,
            }]
        }
        self.unixsocket_path = os.path.join(os.environ['VOLATILE_DIR'], 'haproxy/datadog-haproxy-stats.sock')
        self.unixsocket_url = 'unix://{0}'.format(self.unixsocket_path)
        self.config_unixsocket = {
            'instances': [{
                'url': self.unixsocket_url,
                'collect_aggregates_only': False,
            }]
        }

    def _test_frontend_metrics(self, shared_tag):
        frontend_tags = shared_tag + ['type:FRONTEND', 'service:public']
        for gauge in self.FRONTEND_CHECK_GAUGES:
            self.assertMetric(gauge, tags=frontend_tags, count=1)

        if os.environ.get('FLAVOR_VERSION','1.5.11').split('.')[:2] >= ['1', '4']:
            for gauge in self.FRONTEND_CHECK_GAUGES_POST_1_4:
                self.assertMetric(gauge, tags=frontend_tags, count=1)

        for rate in self.FRONTEND_CHECK_RATES:
            self.assertMetric(rate, tags=frontend_tags, count=1)

        if os.environ.get('FLAVOR_VERSION','1.5.11').split('.')[:2] >= ['1', '4']:
            for rate in self.FRONTEND_CHECK_RATES_POST_1_4:
                self.assertMetric(rate, tags=frontend_tags, count=1)

    def _test_backend_metrics(self, shared_tag, services=None):
        backend_tags = shared_tag + ['type:BACKEND']
        if not services:
            services = self.BACKEND_SERVICES
        for service in services:
            for backend in self.BACKEND_LIST:
                tags = backend_tags + ['service:' + service, 'backend:' + backend]

                for gauge in self.BACKEND_CHECK_GAUGES:
                    self.assertMetric(gauge, tags=tags, count=1)

                if os.environ.get('FLAVOR_VERSION','1.5.11').split('.')[:2] >= ['1', '5']:
                    for gauge in self.BACKEND_CHECK_GAUGES_POST_1_5:
                        self.assertMetric(gauge, tags=tags, count=1)

                for rate in self.BACKEND_CHECK_RATES:
                    self.assertMetric(rate, tags=tags, count=1)

                if os.environ.get('FLAVOR_VERSION','1.5.11').split('.')[:2] >= ['1', '4']:
                    for rate in self.BACKEND_CHECK_RATES_POST_1_4:
                        self.assertMetric(rate, tags=tags, count=1)

    def _test_service_checks(self, services=None):
        if not services:
            services = self.BACKEND_SERVICES
        for service in services:
            for backend in self.BACKEND_LIST:
                tags = ['service:' + service, 'backend:' + backend]
                self.assertServiceCheck(self.check.SERVICE_CHECK_NAME,
                                        status=AgentCheck.UNKNOWN,
                                        count=1,
                                        tags=tags)
            tags = ['service:' + service, 'backend:BACKEND']
            self.assertServiceCheck(self.check.SERVICE_CHECK_NAME,
                                    status=AgentCheck.OK,
                                    count=1,
                                    tags=tags)

    def test_check(self):
        self.run_check_twice(self.config)

        shared_tag = ['instance_url:http://localhost:3835/stats']

        self._test_frontend_metrics(shared_tag)
        self._test_backend_metrics(shared_tag)

        # check was run 2 times
        #       - FRONTEND is reporting OPEN that we ignore
        #       - only the BACKEND aggregate is reporting UP -> OK
        #       - The 3 individual servers are returning no check -> UNKNOWN
        self._test_service_checks()

        # Make sure the service checks aren't tagged with an empty hostname.
        self.assertEquals(self.service_checks[0]['host_name'], get_hostname(config=self.config))

        self.coverage_report()

    def test_check_service_filter(self):
        config = self.config
        config['instances'][0]['services_include'] = ['datadog']
        config['instances'][0]['services_exclude'] = ['.*']
        self.run_check_twice(config)
        shared_tag = ['instance_url:http://localhost:3835/stats']

        self._test_backend_metrics(shared_tag, ['datadog'])

        self._test_service_checks(['datadog'])

        self.coverage_report()

    def test_wrong_config(self):
        config = self.config
        config['instances'][0]['username'] = 'fake_username'

        self.assertRaises(Exception, lambda: self.run_check(config))

        # Test that nothing has been emitted
        self.coverage_report()

    def test_open_config(self):
        self.run_check_twice(self.config_open)

        shared_tag = ['instance_url:http://localhost:3836/stats']

        self._test_frontend_metrics(shared_tag)
        self._test_backend_metrics(shared_tag)
        self._test_service_checks()

        # This time, make sure the hostname is empty
        self.assertEquals(self.service_checks[0]['host_name'], '')

        self.coverage_report()

    def test_unixsocket_config(self):
        if not Platform.is_linux():
            raise SkipTest("Can run only on Linux because of Docker limitations on unix socket sharing")

        self.run_check_twice(self.config_unixsocket)

        shared_tag = ['instance_url:{0}'.format(self.unixsocket_url)]

        self._test_frontend_metrics(shared_tag)
        self._test_backend_metrics(shared_tag)
        self._test_service_checks()

        self.coverage_report()
