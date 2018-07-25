"""Import AIP Django management command: imports an AIP into the Storage
Service.

The user must minimally provide the full path to a locally available AIP via
the ``--aip-path`` argument. The user may also specify the AIP Storage location
UUID indicating where the AIP should be stored (using
``--aip-storage-location``) as well as the UUID of the pipeline that the AIP
was created with (using ``--pipeline``).

The command will:

- validate the AIP (make sure it is a valid Bag),
- move the AIP to the AIP Storage location's local path (note: the AS location
  must be a local filesystem type), and
- add an entry to the locations_package table of the storage service database.

To run this command in an am.git Docker Compose deploy::

    $ make manage-ss ARG='import_aip --aip-path=/home/archivematica/new_uncompressed.tar.gz'

"""

from __future__ import print_function
from __future__ import unicode_literals

import glob
import os
from pwd import getpwnam
import shutil
import subprocess
import tarfile
import tempfile

import bagit
from django.core.management.base import BaseCommand
from django.db.utils import IntegrityError

from administration.models import Settings
from common import utils
from locations import models


DEFAULT_AS_LOCATION = 'default_AS_location'
ANSI_HEADER = '\033[95m'
ANSI_OKGREEN = '\033[92m'
ANSI_WARNING = '\033[93m'
ANSI_FAIL = '\033[91m'
ANSI_ENDC = '\033[0m'


class Command(BaseCommand):

    help = 'Import an AIP into the Storage Service'

    def add_arguments(self, parser):
        parser.add_argument(
            'aip_path', help='Full path to the AIP to be imported')
        parser.add_argument(
            '--aip-storage-location',
            help='UUID of the AIP Storage Location where the imported AIP'
                 ' should be stored. Defaults to default AS location.',
            default=DEFAULT_AS_LOCATION,
            required=False)
        parser.add_argument(
            '--pipeline',
            help='UUID of a pipeline that should be listed as the AIP\'s'
                 ' origin. Defaults to an arbitrary pipeline.',
            required=False)

    def handle(self, *args, **options):
        print(header(
            'Attempting to import the AIP at {}.'.format(options['aip_path'])))
        try:
            import_aip(options['aip_path'],
                       options['aip_storage_location'],
                       options['pipeline'])
        except ImportAIPException as err:
            print(fail(err))


class ImportAIPException(Exception):
    """An error occurred when attempting to import an AIP."""


def header(string):
    return '{}{}{}'.format(ANSI_HEADER, string, ANSI_ENDC)


def okgreen(string):
    return '{}{}{}'.format(ANSI_OKGREEN, string, ANSI_ENDC)


def warning(string):
    return '{}{}{}'.format(ANSI_WARNING, string, ANSI_ENDC)


def fail(string):
    return '{}{}{}'.format(ANSI_FAIL, string, ANSI_ENDC)


def is_compressed(aip_path):
    return os.path.isfile(aip_path)


def tree(path):
    for root, _, files in os.walk(path):
        level = root.replace(path, '').count(os.sep)
        indent = ' ' * 4 * (level)
        print('{}{}/'.format(indent, os.path.basename(root)))
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            print('{}{}'.format(subindent, f))


def decompress(aip_path):
    if not aip_path.endswith('.tar.gz'):
        raise ImportAIPException(
            'Unable to decompress the AIP at {}'.format(aip_path))
    temp_dir = tempfile.mkdtemp()
    with tarfile.open(aip_path) as tar:
        aip_root_dir = os.path.commonprefix(tar.getnames())
        tar.extractall(path=temp_dir)
    return os.path.join(temp_dir, aip_root_dir)


def confirm_aip_exists(aip_path):
    if not os.path.exists(aip_path):
        raise ImportAIPException('There is nothing at {}'.format(aip_path))


def decompress_aip(aip_path):
    if is_compressed(aip_path):
        return decompress(aip_path)
    return aip_path


def validate(aip_path):
    bag = bagit.Bag(aip_path)
    if not bag.is_valid():
        raise ImportAIPException(
            'The AIP at {} is not a valid Bag; aborting.'.format(aip_path))


def get_aip_mets_path(aip_path):
    aip_mets_path = glob.glob(os.path.join(aip_path, 'data', 'METS*xml'))
    if not aip_mets_path:
        raise ImportAIPException(
            'Unable to find a METS file in {}.'.format(aip_path))
    return aip_mets_path[0]


def get_aip_uuid(aip_mets_path):
    return os.path.basename(aip_mets_path)[5:41]


def get_aip_storage_location_path(aip_storage_location_uuid):
    if aip_storage_location_uuid == DEFAULT_AS_LOCATION:
        aip_storage_location_uuid = Settings.objects.get(
            name=aip_storage_location_uuid).value
    try:
        return models.Location.objects.get(
            uuid=aip_storage_location_uuid).full_path
    except models.Location.DoesNotExist:
        raise ImportAIPException(
            'Unable to find an AIP storage location matching {}.'.format(
                aip_storage_location_uuid))


def get_aip_storage_location(aip_storage_location_uuid):
    if aip_storage_location_uuid == DEFAULT_AS_LOCATION:
        aip_storage_location_uuid = Settings.objects.get(
            name=aip_storage_location_uuid).value
    try:
        return models.Location.objects.get(
            uuid=aip_storage_location_uuid)
    except models.Location.DoesNotExist:
        raise ImportAIPException(
            'Unable to find an AIP storage location matching {}.'.format(
                aip_storage_location_uuid))


def mkdir(aip_new_home):
    try:
        os.makedirs(aip_new_home)
    except OSError:
        shutil.rmtree(aip_new_home)
        os.makedirs(aip_new_home)


def fix_ownership(aip_model_inst, aip_storage_location_path):
    first_uuid_dir = aip_model_inst.uuid[0:4]
    am_uid = getpwnam('archivematica').pw_uid
    am_gid = getpwnam('archivematica').pw_gid
    for root, dirs, files in os.walk(
            os.path.join(aip_storage_location_path, first_uuid_dir)):
        os.chown(root, am_uid, am_gid)
        for dir_ in dirs:
            os.chown(os.path.join(root, dir_), am_uid, am_gid)
        for file_ in files:
            os.chown(os.path.join(root, file_), am_uid, am_uid)


def copy_aip_to_aip_storage_location(aip_model_inst, aip_path,
                                     aip_storage_location):
    aip_storage_location_path = aip_storage_location.full_path
    aip_uuid_path = utils.uuid_to_path(aip_model_inst.uuid)
    aip_new_home = os.path.join(aip_storage_location_path, aip_uuid_path)
    mkdir(aip_new_home)
    aip_model_inst.current_path = os.path.join(
        aip_uuid_path, os.path.basename(os.path.normpath(aip_path)))
    copy_rsync(aip_path, os.path.join(aip_storage_location_path,
                                      aip_model_inst.current_path))
    fix_ownership(aip_model_inst, aip_storage_location_path)


def copy_rsync(source, destination):
    source = os.path.join(source, '')
    p = subprocess.Popen(
        ['rsync', '-t', '-O', '--protect-args', '--chmod=ugo+rw', '-r', source,
         destination],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    p.communicate()
    if p.returncode != 0:
        raise ImportAIPException(
            'Unable to move the AIP from {} to {}.'.format(source, destination))


def get_pipeline(adoptive_pipeline_uuid):
    if adoptive_pipeline_uuid:
        try:
            return models.Pipeline.objects.get(uuid=adoptive_pipeline_uuid)
        except models.Pipeline.DoesNotExist:
            raise ImportAIPException(
                'There is no pipeline with uuid {}'.format(
                    adoptive_pipeline_uuid))
    return models.Pipeline.objects.first()


def save_aip_model_instance(aip_model_inst):
    try:
        aip_model_inst.save()
    except IntegrityError:
        models.Package.objects.filter(uuid=aip_model_inst.uuid).delete()
        aip_model_inst.save()


def check_if_aip_already_exists(aip_uuid):
    duplicates = models.Package.objects.filter(uuid=aip_uuid).all()
    if duplicates:
        prompt = warning(
            'An AIP with UUID {} already exists in this Storage Service? If you'
            ' want to import this AIP anyway (and destroy the existing one),'
            ' then enter "y" or "yes": '.format(aip_uuid))
        user_response = raw_input(prompt)
        if user_response.lower() not in ('y', 'yes'):
            raise ImportAIPException(
                'Aborting importation of an already existing AIP')


def import_aip(aip_path, aip_storage_location_uuid, adoptive_pipeline_uuid):
    # NOTE/WARNING: currently assumes that the user-supplied ``aip_path`` is a
    # compressed directory and that the AIP should be decompressed prior to
    # importation.
    confirm_aip_exists(aip_path)
    aip_path = decompress_aip(aip_path)
    validate(aip_path)
    aip_mets_path = get_aip_mets_path(aip_path)
    aip_uuid = get_aip_uuid(aip_mets_path)
    check_if_aip_already_exists(aip_uuid)
    aip_storage_location = get_aip_storage_location(aip_storage_location_uuid)
    aip_model_inst = models.Package(
        uuid=aip_uuid,
        package_type='AIP',
        status='UPLOADED',
        size=utils.recalculate_size(aip_path),
        origin_pipeline=get_pipeline(adoptive_pipeline_uuid),
        current_location=aip_storage_location)
    copy_aip_to_aip_storage_location(aip_model_inst, aip_path,
                                     aip_storage_location)
    save_aip_model_instance(aip_model_inst)
    print(okgreen('Successfully imported AIP {}.'.format(aip_uuid)))
