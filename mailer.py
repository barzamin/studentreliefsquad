import click
import csv
import itertools
from sendgrid import SendGridAPIClient, Mail
import time

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

    from_whom = ('squad@studentrelief.team', 'Student Relief Squad')

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
    for batch in grouper(BATCH_SIZE, segment_rows):
        message = Mail(
            from_email=from_whom,
            to_emails=[x['email'] for x in batch])
        message.template_id = template_id

        if wet_run:
            response = sg.send(message)
            print(f'--> {response.status_code}')

        time.sleep(1) # :3


if __name__ == '__main__':
    mail()