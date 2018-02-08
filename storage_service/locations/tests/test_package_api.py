import base64
import json
import os
import vcr
import shutil

from django.test import TestCase
from django.contrib.auth.models import User

from locations import models

import test_locs 

FIXTURES_DIR = test_locs.FIXTURES_READ_DIR

class TestPackageAPI(TestCase):

    fixtures = ['base.json', 'package.json', 'arkivum.json']

    def setUp(self):
        self.test_location = models.Location.objects.get(uuid='615103f0-0ee0-4a12-ba17-43192d1143ea')
        # Set up locations to point to fixtures directory
        self.test_location.relative_path = FIXTURES_DIR[1:]
        self.test_location.save()
        models.Space.objects.filter(uuid='6fb34c82-4222-425e-b0ea-30acfd31f52e').update(path=FIXTURES_DIR)
        ss_int = models.Location.objects.get(purpose='SS')
        ss_int.relative_path = FIXTURES_DIR[1:]
        ss_int.save()
        # Set Arkivum package request ID
        models.Package.objects.filter(uuid='c0f8498f-b92e-4a8b-8941-1b34ba062ed8').update(misc_attributes={'arkivum_identifier': '2e75c8ad-cded-4f7e-8ac7-85627a116e39'})

        user = User.objects.get(username='test')
        user.set_password('test')
        self.client.defaults['HTTP_AUTHORIZATION'] = 'Basic ' + base64.b64encode('test:test')

    def tearDown(self):
        for entry in os.listdir(FIXTURES_DIR):
            if entry.startswith('tmp'):
                shutil.rmtree(os.path.join(FIXTURES_DIR, entry))

    def test_requires_auth(self):
        del self.client.defaults['HTTP_AUTHORIZATION']
        urls = [
            '/api/v2/file/metadata/',
            '/api/v2/file/e0a41934-c1d7-45ba-9a95-a7531c063ed1/contents/',
            '/api/v2/file/6aebdb24-1b6b-41ab-b4a3-df9a73726a34/download/',
            '/api/v2/file/0d4e739b-bf60-4b87-bc20-67a379b28cea/extract_file/',
        ]
        # Get metadata
        for url in urls:
            response = self.client.get(url)
            assert response.status_code == 401

    def test_file_data_returns_metadata_given_relative_path(self):
        path = 'test_sip/objects/file.txt'
        response = self.client.get('/api/v2/file/metadata/',
                                   {'relative_path': path})
        assert response.status_code == 200
        assert response['content-type'] == 'application/json'
        body = json.loads(response.content)
        assert body[0]['relative_path'] == path
        assert body[0]['fileuuid'] == '86bfde11-e2a1-4ee7-b98d-9556b5f05198'

    def test_file_data_returns_bad_response_with_no_accepted_parameters(self):
        response = self.client.post('/api/v2/file/metadata/')
        assert response.status_code == 400

    def test_file_data_returns_404_if_no_file_found(self):
        response = self.client.get('/api/v2/file/metadata/', {'fileuuid': 'nosuchfile'})
        assert response.status_code == 404

    def test_package_contents_returns_metadata(self):
        response = self.client.get('/api/v2/file/e0a41934-c1d7-45ba-9a95-a7531c063ed1/contents/')
        assert response.status_code == 200
        assert response['content-type'] == 'application/json'
        body = json.loads(response.content)
        assert body['success'] is True
        assert len(body['files']) == 1
        assert body['files'][0]['name'] == 'test_sip/objects/file.txt'

    def test_adding_package_files_returns_400_with_empty_post_body(self):
        response = self.client.put('/api/v2/file/e0a41934-c1d7-45ba-9a95-a7531c063ed1/contents/',
                                   data="", content_type="application/json")
        assert response.status_code == 400

    def test_adding_package_files_returns_400_if_post_body_is_not_json(self):
        response = self.client.put('/api/v2/file/e0a41934-c1d7-45ba-9a95-a7531c063ed1/contents/',
                                   data="not json!",
                                   content_type="application/json")
        assert response.status_code == 400

    def test_adding_package_files_returns_400_if_post_body_is_not_a_list(self):
        response = self.client.put('/api/v2/file/e0a41934-c1d7-45ba-9a95-a7531c063ed1/contents/',
                                   data="{}", content_type="application/json")
        assert response.status_code == 400

    def test_adding_package_files_returns_400_if_expected_fields_are_missing(self):
        body = [{
            "relative_path": "/dev/null"
        }]
        response = self.client.put('/api/v2/file/e0a41934-c1d7-45ba-9a95-a7531c063ed1/contents/',
                                   data=json.dumps(body),
                                   content_type="application/json")
        assert response.status_code == 400

    def test_adding_files_to_package_returns_200_for_empty_list(self):
        response = self.client.put('/api/v2/file/79245866-ca80-4f84-b904-a02b3e0ab621/contents/',
                                   data='[]', content_type="application/json")
        assert response.status_code == 200

    def test_adding_files_to_package(self):
        p = models.Package.objects.get(uuid="79245866-ca80-4f84-b904-a02b3e0ab621")
        assert p.file_set.count() == 0

        body = [
            {
                "relative_path": "empty-transfer-79245866-ca80-4f84-b904-a02b3e0ab621/1.txt",
                "fileuuid": "7bffcce7-63f5-4b2e-af57-d266bfa2e3eb",
                "accessionid": "",
                "sipuuid": "79245866-ca80-4f84-b904-a02b3e0ab621",
                "origin": "36398145-6e49-4b5b-af02-209b127f2726",
            },
            {
                "relative_path": "empty-transfer-79245866-ca80-4f84-b904-a02b3e0ab621/2.txt",
                "fileuuid": "152be912-819f-49c4-968f-d5ce959c1cb1",
                "accessionid": "",
                "sipuuid": "79245866-ca80-4f84-b904-a02b3e0ab621",
                "origin": "36398145-6e49-4b5b-af02-209b127f2726",
            },
        ]

        response = self.client.put('/api/v2/file/79245866-ca80-4f84-b904-a02b3e0ab621/contents/',
                                   data=json.dumps(body),
                                   content_type="application/json")
        assert response.status_code == 201
        assert p.file_set.count() == 2

    def test_removing_file_from_package(self):
        p = models.Package.objects.get(uuid="a59033c2-7fa7-41e2-9209-136f07174692")
        assert p.file_set.count() == 1

        response = self.client.delete('/api/v2/file/a59033c2-7fa7-41e2-9209-136f07174692/contents/')
        assert response.status_code == 204
        assert p.file_set.count() == 0

    def test_download_compressed_package(self):
        """ It should return the package. """
        response = self.client.get('/api/v2/file/6aebdb24-1b6b-41ab-b4a3-df9a73726a34/download/')
        assert response.status_code == 200
        assert response['content-type'] == 'application/zip'
        assert response['content-disposition'] == 'attachment; filename="working_bag.zip"'

    def test_download_uncompressed_package(self):
        """ It should tar a package before downloading. """
        response = self.client.get('/api/v2/file/0d4e739b-bf60-4b87-bc20-67a379b28cea/download/')
        assert response.status_code == 200
        assert response['content-type'] == 'application/x-tar'
        assert response['content-disposition'] == 'attachment; filename="working_bag.tar"'
        content = ''.join(response.streaming_content)  # Convert to one string
        assert 'bag-info.txt' in content
        assert 'bagit.txt' in content
        assert 'manifest-md5.txt' in content
        assert 'tagmanifest-md5.txt' in content
        assert 'test.txt' in content

    def test_download_lockss_chunk_incorrect(self):
        """ It should default to the local path if a chunk ID is provided but package isn't in LOCKSS. """
        response = self.client.get('/api/v2/file/0d4e739b-bf60-4b87-bc20-67a379b28cea/download/', data={'chunk_number': 1})
        assert response.status_code == 200
        assert response['content-type'] == 'application/x-tar'
        assert response['content-disposition'] == 'attachment; filename="working_bag.tar"'
        content = ''.join(response.streaming_content)  # Convert to one string
        assert 'bag-info.txt' in content
        assert 'bagit.txt' in content
        assert 'manifest-md5.txt' in content
        assert 'tagmanifest-md5.txt' in content
        assert 'test.txt' in content

    def test_download_package_not_exist(self):
        """ It should return 404 for a non-existant package. """
        response = self.client.get('/api/v2/file/dnednedn-edne-dned-nedn-ednednednedn/download/', data={'chunk_number': 1})
        assert response.status_code == 404

    @vcr.use_cassette(os.path.join(FIXTURES_DIR, 'vcr_cassettes', 'arkivum_update_package_status.yaml'))
    def test_download_package_arkivum_not_available(self):
        """ It should return 202 if the file is in Arkivum but only on tape. """
        response = self.client.get('/api/v2/file/c0f8498f-b92e-4a8b-8941-1b34ba062ed8/download/')
        assert response.status_code == 202
        j = json.loads(response.content)
        assert j['error'] is False
        assert j['message'] == 'File is not locally available.  Contact your storage administrator to fetch it.'

    @vcr.use_cassette(os.path.join(FIXTURES_DIR, 'vcr_cassettes', 'api_download_package_arkivum_error.yaml'))
    def test_download_package_arkivum_error(self):
        """ It should return 502 error from Arkivum. """
        response = self.client.get('/api/v2/file/c0f8498f-b92e-4a8b-8941-1b34ba062ed8/download/')
        assert response.status_code == 502
        j = json.loads(response.content)
        assert j['error'] is True
        assert 'Error' in j['message'] and 'Arkivum' in j['message']

    def test_download_file_no_path(self):
        """ It should return 400 Bad Request """
        response = self.client.get('/api/v2/file/0d4e739b-bf60-4b87-bc20-67a379b28cea/extract_file/')
        assert response.status_code == 400
        assert 'relative_path_to_file' in response.content

    def test_download_file_from_compressed(self):
        """ It should extract and return the file. """
        response = self.client.get('/api/v2/file/6aebdb24-1b6b-41ab-b4a3-df9a73726a34/extract_file/', data={'relative_path_to_file': 'working_bag/data/test.txt'})
        assert response.status_code == 200
        assert response['content-type'] == 'text/plain'
        assert response['content-disposition'] == 'attachment; filename="test.txt"'
        content = ''.join(response.streaming_content)  # Convert to one string
        assert content == 'test'

    def test_download_file_from_uncompressed(self):
        """ It should return the file. """
        response = self.client.get('/api/v2/file/0d4e739b-bf60-4b87-bc20-67a379b28cea/extract_file/', data={'relative_path_to_file': 'working_bag/data/test.txt'})
        assert response.status_code == 200
        assert response['content-type'] == 'text/plain'
        assert response['content-disposition'] == 'attachment; filename="test.txt"'
        content = ''.join(response.streaming_content)  # Convert to one string
        assert content == 'test'

    @vcr.use_cassette(os.path.join(FIXTURES_DIR, 'vcr_cassettes', 'arkivum_update_package_status.yaml'))
    def test_download_file_arkivum_not_available(self):
        """ It should return 202 if the file is in Arkivum but only on tape. """
        response = self.client.get('/api/v2/file/c0f8498f-b92e-4a8b-8941-1b34ba062ed8/extract_file/', data={'relative_path_to_file': 'working_bag/data/test.txt'})
        assert response.status_code == 202
        j = json.loads(response.content)
        assert j['error'] is False
        assert j['message'] == 'File is not locally available.  Contact your storage administrator to fetch it.'

    @vcr.use_cassette(os.path.join(FIXTURES_DIR, 'vcr_cassettes', 'api_download_package_arkivum_error.yaml'))
    def test_download_file_arkivum_error(self):
        """ It should return 502 error from Arkivum. """
        response = self.client.get('/api/v2/file/c0f8498f-b92e-4a8b-8941-1b34ba062ed8/extract_file/', data={'relative_path_to_file': 'working_bag/data/test.txt'})
        assert response.status_code == 502
        j = json.loads(response.content)
        assert j['error'] is True
        assert 'Error' in j['message'] and 'Arkivum' in j['message']