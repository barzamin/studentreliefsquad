import click
import csv
import itertools
from sendgrid import SendGridAPIClient, Mail, Personalization, To, Asm, GroupId
import time
import json
from python_http_client import exceptions

def grouper(n, iterable):
    it = iter(iterable)
    while True:
       chunk = tuple(itertools.islice(it, n))
       if not chunk:
           return
       yield chunk

@click.command()
@click.argument('listcsv', type=click.File('r'))
@click.argument('template-id')
@click.option('-s', '--segment', multiple=True)
@click.option('--sendgrid-key', envvar='SENDGRID_API_KEY')
@click.option('--wet-run/--dry-run', default=False)
def mail(listcsv, template_id, segment, sendgrid_key, wet_run):
    rdr = csv.DictReader(listcsv)

    from_whom = ('squad@studentrelief.team', 'UW Student Relief Squad')

    segment_rows = []
    for row in rdr:
        def parse(predicate):
            return [x.strip() for x in predicate.split('=')]
        def matches(row, k, v):
            return row.get(k) == v
        if all([matches(row, *parse(pred)) for pred in segment]):
            segment_rows.append(row)

    print(f'--> sending to {len(segment_rows)} matching addresses')

    sg = SendGridAPIClient(api_key=sendgrid_key)

    BATCH_SIZE = 500
    N_BATCHES = len(segment_rows)//BATCH_SIZE
    for i, batch in enumerate(grouper(BATCH_SIZE, segment_rows)):
        print(f'  [batch {i}/{N_BATCHES}]')
        message = Mail(
            from_email=from_whom)
        for recipient in batch:
            pers = Personalization()
            pers.add_email(To(recipient['email']))
            message.add_personalization(pers)
        message.template_id = template_id
        message.asm = Asm(GroupId(136302))

        if wet_run:
            try:
                response = sg.send(message)
                print(f'--> {response.status_code}')
            except exceptions.BadRequestsError as e:
                print(e.body)
                exit()

        time.sleep(1) # :3


if __name__ == '__main__':
    mail()