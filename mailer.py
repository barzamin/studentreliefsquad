import click
import csv

@click.command()
@click.argument('listcsv', type=click.File('r'))
@click.option('-s', '--segment', multiple=True)
def mail(listcsv, segment):
    rdr = csv.DictReader(listcsv)
    # print(segment)

    for row in rdr:
        def parse(predicate):
            return [x.strip() for x in predicate.split('=')]
        def matches(row, k, v):
            return row.get(k) == v
        if all([matches(row, *parse(pred)) for pred in segment]):
            print(row)


if __name__ == '__main__':
    mail()