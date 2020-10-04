#!/usr/bin/python
import os

REPO = [
    'aws_app_rust',
    'calendar_app_rust',
    'diary_app_rust',
    'garmin_rust',
    'movie_collection_rust',
    'podcatch_rust',
    'security_log_analysis_rust',
    'sync_app_rust',
]
HOME = os.environ['HOME']

def generate_postgres_toml():
    for repo in REPO:
        config = f'{HOME}/.config/{repo}/config.env'
        backup_db = f'{HOME}/setup_files/build/{repo}/scripts/backup_db.sh'
        restore_db = f'{HOME}/setup_files/build/{repo}/scripts/restore_db.sh'
        db_url = None
        bucket = None
        tables = None
        sequences = {}
        if os.path.exists(config) and os.path.exists(backup_db):
            config = open(config, 'r')
            for line in config:
                if 'DATABASE_URL' in line or 'PGURL' in line:
                    db_url = line.split('=')[1].strip()
            for line in open(backup_db, 'r'):
                if line.startswith('BUCKET'):
                    bucket=line.split('=')[1].strip().replace('"', '')
                if line.startswith('TABLES'):
                    tables = []
                elif isinstance(tables, list):
                    if line.strip() == '"':
                        break
                    else:
                        tables.append(line.strip().replace('"', '').replace("'", ''))
            if os.path.exists(restore_db):
                for line in open(restore_db):
                    if 'setval' in line:
                        seq = None
                        max = None
                        table = None
                        for word in line.split(' '):
                            if 'setval' in word:
                                seq = word.replace('setval(', '').replace("'", '').replace(',', '').replace('"', '')
                            elif 'max' in word:
                                max = word.strip().replace('max(', '').replace(')', '')
                            elif seq is not None and max is not None and table is None and word.lower() != 'from':
                                table = word.strip().replace(')', '').replace(',', '').replace('"', '')
                        sequences[seq] = (table, max)
            print(f'[{repo}]')
            print(f"database_url = '{db_url}'")
            print(f"destination = 's3://{bucket}'")
            print(f"tables = {tables}")
            if sequences:
                sequences = '{' + ', '.join(f"{k}=['{t}', '{v}']" for (k, (t, v)) in sequences.items()) + '}'
                print(f"sequences = {sequences}")
            print("")

def prettify_toml():
    import toml

generate_postgres_toml()