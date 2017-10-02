#!/usr/bin/env python2.7
import subprocess
import shutil
import sys
import zipfile
import logging
import os
import tempfile
from flask import Flask, jsonify, abort
from flask import send_file, request, make_response
from werkzeug.utils import secure_filename

app = Flask(__name__)

arangodump = '/usr/bin/arangodump'
arangorestore = '/usr/bin/arangorestore'
arangopw = os.environ['ARANGO_ROOT_PASSWORD']
arangodumpdir = '/home/arangodb/arangodb-dump'

@app.route('/dump/<db>', methods=['GET'])
def dump(db):
    try:
        # Dump the database from ArangoDB.
        outdir = arangodumpdir + '/dump-topology'
        arangodump_zipfile = arangodumpdir + '/dump'
        arangodump_zipfile_withext = arangodump_zipfile + '.zip'
        logging.info(outdir)
        retcode = subprocess.call(dumpcmd(db, outdir), shell=True)
        if retcode == 0:
            logging.info(sys.stdout)
        else:
            logging.error(sys.stderr)
            abort(500, {'message': 'Failed to dump topology %s' % db})

        # Compress the dumped folder into a zip file
        shutil.make_archive(arangodump_zipfile, 'zip', outdir);
    except OSError as e:
        print >>sys.stderr, "Execution failed:", e
        abort(500)
    finally:
        # Remove the dumping folder
        shutil.rmtree(outdir)

    try:
        return send_file(arangodump_zipfile_withext, attachment_filename='dump.zip')
    finally:
        # Remove the zip file
        os.remove(arangodump_zipfile_withext)
        logging.info('Dumping finished')

@app.route('/restore/<db>', methods=['POST'])
def restore(db):
    logging.info('Restoring database %s...', db)
    if 'file' not in request.files:
        logging.error('\'file\' parameter not found in request\'s list of files.')
        abort(400, {'message' : 'Input does not have a \'file\' parameter set to a file resource'})

    dbZipFile = request.files['file']

    # Check if input is a zip file.
    if zipfile.is_zipfile(dbZipFile) == False:
        logging.info('Input is not a zip file')
        abort(400, {'message': 'input is not a zip file'})

    with zipfile.ZipFile(dbZipFile, 'r') as z:
        # Test if the zip file is valid
        if z.testzip() == None:
            restoredir = tempfile.mkdtemp('restore-topology')
            logging.info(restoredir)
            z.extractall(restoredir)
        else:
            logging.info('Invalid zip file ' + z.testzip())
            abort(400, {'message': 'Invalid zip file'})

    try:
        restore_cmd = restorecmd(db, restoredir)
        logging.info('Running %s...', restore_cmd)
        # Run arangorestore to restore the database specified in the args
        retcode = subprocess.call(restore_cmd, shell=True)
        if retcode == 0:
            logging.info(sys.stdout)
        else:
            logging.error(sys.stderr)
            abort(500, {'message': 'Failed to restore topology %s' % db})
    except OSError as e:
        print >>sys.stderr, "Execution failed:", e
        abort(500)
    finally:
        # Remove the restored folder
        shutil.rmtree(restoredir)

    return make_response('Restored database successfully', 201)

def dumpcmd(db, outdir):
    cmd = [arangodump]
    cmd.append("--server.username=root")
    cmd.append("--server.password=" + arangopw)
    cmd.append("--overwrite=true")
    cmd.append("--include-system-collections=true")
    cmd.append("--output-directory=" + outdir)
    cmd.append("--server.database=" + db)
    return ' '.join(cmd)

def restorecmd(db, indir):
    cmd = [arangorestore]
    cmd.append("--server.username=root")
    cmd.append("--server.password=" + arangopw)
    cmd.append("--overwrite=true")
    cmd.append("--create-database=true")
    cmd.append("--include-system-collections=true")
    cmd.append("--input-directory=" + indir)
    cmd.append("--server.database=" + db)
    return ' '.join(cmd)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s : %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p')

    app.run(host='0.0.0.0',
            port=8599,
            debug=False)
