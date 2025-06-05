# content of test_sysexit.py
import os
import sys
import shutil
import hashlib
import copy
import pytest
try:
    import unittest.mock as umock
except ImportError:
    from unittest import mock as umock

sys.modules['opensearchpy'] = umock.Mock()
sys.modules['opensearchpy.exceptions'] = umock.Mock()


class TestPreProcessingChecksum:
    def constructor(
        self,
    ):  # PyTest doesnt allow __init__ constructor so this is a workaround
        self.test_directory = "test_directory"
        self.test_job_json = {
            "job_info": {"job_dir": "/tmp"},
            "localize_urls": [
                {"url": "test_1.zip.md5"},
                {"url": "test_1.zip"},
                {"url": "test_2.zip.md5"},
                {"url": "test_2.zip"},
                {"url": self.test_directory},
            ],
        }
        self.test_file_in_directory_md5_1 = os.path.join(
            "/tmp", self.test_directory, "test_3.zip.md5"
        )
        self.test_file_in_directory_1 = os.path.join(
            "/tmp", self.test_directory, "test_3.zip"
        )
        self.test_file_in_directory_md5_2 = os.path.join(
            "/tmp", self.test_directory, "test_4.zip.md5"
        )
        self.test_file_in_directory_2 = os.path.join(
            "/tmp", self.test_directory, "test_4.zip"
        )

    def create_test_directory_and_files(self):
        try:
            os.mkdir(os.path.join("/tmp", self.test_directory))
        except:  # directory already exists
            pass

        with open(self.test_file_in_directory_md5_1, "w") as f:
            f.write("5c9bee4c87b80170f57f1ef696fa1992")
        with open(self.test_file_in_directory_md5_2, "w") as f:
            f.write("4e6e202a8566ff893d4b806d3eee3a41")
        with open(self.test_file_in_directory_1, "w") as f:
            f.write("testing testing 123")
        with open(self.test_file_in_directory_2, "w") as f:
            f.write("testing testing 1234")

        with open("/tmp/test_1.zip.md5", "w") as f:
            f.write("4e6e202a8566ff893d4b806d3eee3a41")
        with open("/tmp/test_1.zip", "w") as f:
            f.write("testing testing 1234")
        with open("/tmp/test_2.zip.md5", "w") as f:
            f.write("05d641836d3c1869a170b386a7a41c56")
        with open("/tmp/test_2.zip", "w") as f:
            f.write("testing testing 12345")

    def clear_test_directory_and_files(self):
        os.remove("/tmp/test_1.zip.md5")
        os.remove("/tmp/test_1.zip")
        os.remove("/tmp/test_2.zip.md5")
        os.remove("/tmp/test_2.zip")
        shutil.rmtree(os.path.join("/tmp", self.test_directory))

    def test_hashlib_mapper(self):
        from hysds.utils import hashlib_mapper

        with pytest.raises(Exception):
            hashlib_mapper("non-existing-hashing-algorithm")

        assert hashlib_mapper("md5").name == hashlib.md5().name
        assert hashlib_mapper("MD5").name == hashlib.md5().name
        assert hashlib_mapper("sha1").name == hashlib.sha1().name
        assert hashlib_mapper("SHA1").name == hashlib.sha1().name
        assert hashlib_mapper("sha224").name == hashlib.sha224().name
        assert hashlib_mapper("SHA224").name == hashlib.sha224().name
        assert hashlib_mapper("sha256").name == hashlib.sha256().name
        assert hashlib_mapper("SHA256").name == hashlib.sha256().name
        assert hashlib_mapper("sha384").name == hashlib.sha384().name
        assert hashlib_mapper("SHA384").name == hashlib.sha384().name
        assert hashlib_mapper("sha512").name == hashlib.sha512().name
        assert hashlib_mapper("SHA512").name == hashlib.sha512().name

        # hashlib supports more hashing algorithms in python 3:
        #   sha3_224 sha3_256, sha3_384, blake2b, blake2s, sha3_512, shake_256, shake_128
        if sys.version_info[:2] >= (3, 0):
            assert hashlib_mapper("sha3_224").name == hashlib.sha3_224().name
            assert hashlib_mapper("SHA3_224").name == hashlib.sha3_224().name
            assert hashlib_mapper("sha3_256").name == hashlib.sha3_256().name
            assert hashlib_mapper("SHA3_256").name == hashlib.sha3_256().name
            assert hashlib_mapper("sha3_384").name == hashlib.sha3_384().name
            assert hashlib_mapper("SHA3_384").name == hashlib.sha3_384().name
            assert hashlib_mapper("blake2b").name == hashlib.blake2b().name
            assert hashlib_mapper("BLAKE2B").name == hashlib.blake2b().name
            assert hashlib_mapper("blake2s").name == hashlib.blake2s().name
            assert hashlib_mapper("BLAKE2S").name == hashlib.blake2s().name
            assert hashlib_mapper("sha3_512").name == hashlib.sha3_512().name
            assert hashlib_mapper("SHA3_512").name == hashlib.sha3_512().name
            assert hashlib_mapper("shake_256").name == hashlib.shake_256().name
            assert hashlib_mapper("SHAKE_256").name == hashlib.shake_256().name
            assert hashlib_mapper("shake_128").name == hashlib.shake_128().name
            assert hashlib_mapper("SHAKE_128").name == hashlib.shake_128().name

    def test_calculate_checksum_from_localized_file(self):
        from hysds.utils import calculate_checksum_from_localized_file

        test_file_name = "/tmp/test_calculate_checksum_from_localized_file.zip"
        test_string = "testing testing 123"

        with open(
            test_file_name, "w"
        ) as f:  # creating a test file with random stuff to md5 hash
            f.write(test_string)

        assert (
            calculate_checksum_from_localized_file(test_file_name, "md5")
            == "5c9bee4c87b80170f57f1ef696fa1992"
        )
        assert (
            calculate_checksum_from_localized_file(test_file_name, "sha1")
            == "89512ac8c5fa4e72773049beea2549c2dc3e8dd3"
        )
        assert (
            calculate_checksum_from_localized_file(test_file_name, "sha224")
            == "1c8c0df757fb6915b89d468bb07d0df183722217a96b837cc1bdd485"
        )
        assert (
            calculate_checksum_from_localized_file(test_file_name, "sha256")
            == "7b6f7e8c7911a867c82e5be63ffcb330b3c8fff7528a58b83f74a1304fd05566"
        )
        assert (
            calculate_checksum_from_localized_file(test_file_name, "sha384")
            == "4b9d911363d3e5f1bcebe5490e813ace3de464c90e6f914473d146ce165a17f9515117ce5e2df0d0fef994a234a5364c"
        )
        assert (
            calculate_checksum_from_localized_file(test_file_name, "sha512")
            == "efc75d56682a615a61de8626075fbaa799fe57e991644fc1b4e6e9d0004b7239f32467aa20c7189f8e4827e189f30f755977fc6fa3464133fbd213d571d77e97"
        )

        if sys.version_info[:2] >= (3, 0):
            assert (
                calculate_checksum_from_localized_file(test_file_name, "sha3_224")
                == "8374697d745e2a5155d04b28fb39910c923920ff7cc15219329c7f76"
            )
            assert (
                calculate_checksum_from_localized_file(test_file_name, "sha3_256")
                == "8317ca633d3f8a98cc2de2f9fb5b675d5afa8542031dc9040352d3668423bb56"
            )
            assert (
                calculate_checksum_from_localized_file(test_file_name, "sha3_384")
                == "875b7bc1dcd7c26c66b58ee91aa37b83a3f086d709054d496eba9a75cd1d8f8c0b4f44ef8c7fd41f1ba0a87d5c4fcb92"
            )
            assert (
                calculate_checksum_from_localized_file(test_file_name, "sha3_512")
                == "5bb79282e3c8aabd5aa56c474bc16f43417961f2934c00c4062d2832848a19fe34d6e0d153b01c9f5809059a8ea24bb3c8d85948b10b19492ea1c4ac7b18a551"
            )
            assert (
                calculate_checksum_from_localized_file(test_file_name, "blake2b")
                == "e0602b05f20ff6ec967dad2f7459f1c830f89becde8f51df8de6883833239ea211b59f71503ed46fe2161d2fbe3c9debb93826490d37fb209f9dc07442f5d60f"
            )
            assert (
                calculate_checksum_from_localized_file(test_file_name, "blake2s")
                == "735329022444d0bb427454ae66ef1f0479387b13af0100e53f59319491f1100a"
            )
            assert (
                calculate_checksum_from_localized_file(test_file_name, "shake_256")
                == "1d52cf879f8cd17c6a9293d346c35bc1fdf9d67794d818ddffa5c3fa8221a4ab73ddb736cb3afc20843f0383bd4ee7336b0dd64dc9bc8999f68a24a8f39c65b58653474a40fed0143601c041f7f61a98f5f3611b88346233b5c8167bb88ad81fa7edac034f9eb03f132cd166d9feec779ca6d71e9a3e45eece962afa02ee17ab24de7cc4ca9a6be277323d9c71ed5804f8a98418972769ed57a309f266ff58d1be9c5b6517106cb98cf90881ab69d828446ffb9545f81d9f9c3caa70d85265005b3857d0b0ea164d9641a5e37ed3a347b010202b99115637f0921388d266e7dc79ceacfbcd074fff9bab5207f5bbc73e169f5843ab414a5615136f0f4988d5"
            )
            assert (
                calculate_checksum_from_localized_file(test_file_name, "shake_128")
                == "4e319ddc183f6753409d8c80f7524b17dd3b8754595648b8789cf507be31c7891502b347fb37243b8fc534d61bb27ddf3cc3ee3a3598449e532015509845e57bc7a3afa0133f70f86a2e6c7eb304b57f1ffe5b7105349a5a3238a7a34cae5e89ad0bb24d63757790f3340e4c520120e5ce587486f0a31342ce3e152299006d5d87c4bf6aac17d558a85061329feaf1575479111c5f229f8d8f72c6026d663ea1c8a952ffe9d289db769ab4a6cde6e14df78dd9c112ffbc3e49219f9aaa4db413666164c8c5132dafb1d4a65139a00b623903f8879cd77b1403c696c9fc49e4345da45c1af5d83bd03bf3309e28823d7388c44c7d05771c7bcf3d83a6e8d2c6"
            )
        os.remove(test_file_name)

    def test_generate_list_checksum_files(self):
        from hysds.utils import generate_list_checksum_files

        self.constructor()
        self.create_test_directory_and_files()
        list_checksums = generate_list_checksum_files(self.test_job_json)
        self.clear_test_directory_and_files()

        assert len(list_checksums) == 4

        algorithms_guaranteed = hashlib.algorithms_guaranteed
        for checksum_file in list_checksums:
            algo_type = checksum_file["algo"]
            assert algo_type in algorithms_guaranteed

    def test_validate_checksum_files(self):
        from hysds.utils import validate_checksum_files

        self.constructor()
        self.create_test_directory_and_files()
        validate_checksum_files(self.test_job_json, {})
        self.clear_test_directory_and_files()

    def test_validate_checksum_files_error(self):
        from hysds.utils import validate_checksum_files

        self.constructor()
        self.create_test_directory_and_files()

        error_test_file = "/tmp/test_err.zip"
        error_test_file_md5 = "/tmp/test_err.zip.md5"

        with open(error_test_file, "w") as f:
            f.write("testing testing 123")

        with open(error_test_file_md5, "w") as f:
            f.write("jfdslkfjdslkfdjsfljfdlsjfsjfl")

        test_job_json = copy.deepcopy(self.test_job_json)
        test_job_json["localize_urls"].append({"url": error_test_file})
        test_job_json["localize_urls"].append({"url": error_test_file_md5})

        with pytest.raises(Exception):
            validate_checksum_files(test_job_json)

        self.clear_test_directory_and_files()
        os.remove(error_test_file_md5)
        os.remove(error_test_file)
