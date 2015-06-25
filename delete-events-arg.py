#!/usr/bin/env python

from datetime import date, timedelta
import requests
import time
import json
import sys
from optparse import OptionParser

DEFAULT_DAYS_TO_KEEP = 60
QUERY_ROWS = 25000

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))

def main():
    parser = OptionParser()
    parser.add_option("-a", "--api-key", help="API Key (required)")
    parser.add_option("-d", "--days", type="int", default=DEFAULT_DAYS_TO_KEEP)
    options, args = parser.parse_args()
    if len(args) < 1:
        parser.error("Missing organization(s)")
    if not options.api_key:
        parser.error("Missing required argument -a/--api-key")

    for org_id in args:
        url = 'https://api.boundary.com/%s/events' % org_id
        auth = requests.auth.HTTPBasicAuth(options.api_key, '')
        cutoff = date.today()-timedelta(days=options.days+1)
        print cutoff
        session = requests.session()
        payload = {
#            'q': 'lastSeenAt:[* TO %sT23:59:59Z] AND status:OPEN' % cutoff.isoformat(),
            'q': 'lastSeenAt:[* TO %sT23:59:59Z]' % cutoff.isoformat(),
            'rows': QUERY_ROWS,
            'fl': ['id'],
        }
        delete_headers = {
            'Content-Type': 'application/json',
        }
        response = session.get(url, params=payload, auth=auth)
        while response.status_code == 200 and response.json()["total"]:
            print "Total events: %s" % response.json()["total"]
            ids = [obj['id'] for obj in response.json()["results"]]
            for chunk in chunker(ids, 5000):
                delete_response = session.delete(url, data=json.dumps(chunk),
                    auth=auth, headers=delete_headers)
                if delete_response.status_code != 204:
                    print "Status code: %d, Response: \n%s" % (
                        delete_response.status_code, delete_response.text)
                    sys.exit(1)
                print delete_response
                time.sleep(5)
            if response.json()["total"] < QUERY_ROWS:
                break
            response = session.get(url, params=payload, auth=auth)
        print response

if __name__ == '__main__':
    main()
