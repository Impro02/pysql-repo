# TESTS
from tests.utils import SavedPath, TestCustom, load_expected_data


class TestUsers(TestCustom):
    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_get_all(self, expected_data, saved_path):
        # WHEN
        users = self._user_service.get_users()
        users_dict = [item.model_dump() for item in users]

        self.save_data_json(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_ids_in(self, expected_data, saved_path):
        # GIVEN
        ids_in = [2, 3]

        # WHEN
        users = self._user_service.get_users(ids_in=ids_in)
        users_dict = [item.model_dump() for item in users]

        self.save_data_json(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_ids_not_in(self, expected_data, saved_path):
        # GIVEN
        ids_not_in = [2, 3]

        # WHEN
        users = self._user_service.get_users(ids_not_in=ids_not_in)
        users_dict = [item.model_dump() for item in users]

        self.save_data_json(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_emails_iin(self, expected_data, saved_path):
        # GIVEN
        emails_iin = ["fOO@TEst.coM", "ZOo@TEST.CoM"]

        # WHEN
        users = self._user_service.get_users(emails_iin=emails_iin)
        users_dict = [item.model_dump() for item in users]

        self.save_data_json(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_emails_in(self, expected_data, saved_path):
        # GIVEN
        emails_in = ["foo@test.com", "ZOo@TEST.CoM"]

        # WHEN
        users = self._user_service.get_users(emails_in=emails_in)
        users_dict = [item.model_dump() for item in users]

        self.save_data_json(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_emails_not_iin(self, expected_data, saved_path):
        # GIVEN
        emails_not_iin = ["fOO@test.com", "ZOo@TEST.CoM"]

        # WHEN
        users = self._user_service.get_users(emails_not_iin=emails_not_iin)
        users_dict = [item.model_dump() for item in users]

        self.save_data_json(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_emails_not_in(self, expected_data, saved_path):
        # GIVEN
        emails_not_in = ["foo@test.com", "ZOo@TEST.CoM"]

        # WHEN
        users = self._user_service.get_users(emails_not_in=emails_not_in)
        users_dict = [item.model_dump() for item in users]

        self.save_data_json(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_email_ilike(self, expected_data, saved_path):
        # GIVEN
        email_ilike = "BOo%"

        # WHEN
        users = self._user_service.get_users(email_ilike=email_ilike)
        users_dict = [item.model_dump() for item in users]

        self.save_data_json(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_email_like(self, expected_data, saved_path):
        # GIVEN
        email_like = "boo%"

        # WHEN
        users = self._user_service.get_users(email_like=email_like)
        users_dict = [item.model_dump() for item in users]

        self.save_data_json(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_email_not_like(self, expected_data, saved_path):
        # GIVEN
        email_not_ilike = "BOo%"

        # WHEN
        users = self._user_service.get_users(email_not_ilike=email_not_ilike)
        users_dict = [item.model_dump() for item in users]

        self.save_data_json(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_email_not_like(self, expected_data, saved_path):
        # GIVEN
        email_not_like = "boo%"

        # WHEN
        users = self._user_service.get_users(email_not_like=email_not_like)
        users_dict = [item.model_dump() for item in users]

        self.save_data_json(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_email_equal(self, expected_data, saved_path):
        # GIVEN
        email_equal = "zoo@test.com"

        # WHEN
        users = self._user_service.get_users(email_equal=email_equal)
        users_dict = [item.model_dump() for item in users]

        self.save_data_json(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_email_equal_wrong(self, expected_data, saved_path):
        # GIVEN
        email_equal = "zoo@test.c%"

        # WHEN
        users = self._user_service.get_users(email_equal=email_equal)
        users_dict = [item.model_dump() for item in users]

        self.save_data_json(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_email_iequal_wrong(self, expected_data, saved_path):
        # GIVEN
        email_iequal = "zOO@test.c%"

        # WHEN
        users = self._user_service.get_users(email_iequal=email_iequal)
        users_dict = [item.model_dump() for item in users]

        self.save_data_json(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_email_different(self, expected_data, saved_path):
        # GIVEN
        email_different = "zoo@test.com"

        # WHEN
        users = self._user_service.get_users(email_different=email_different)
        users_dict = [item.model_dump() for item in users]

        self.save_data_json(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_email_different_wrong(self, expected_data, saved_path):
        # GIVEN
        email_different = "zoo@test.c%"

        # WHEN
        users = self._user_service.get_users(email_different=email_different)
        users_dict = [item.model_dump() for item in users]

        self.save_data_json(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_email_idifferent(self, expected_data, saved_path):
        # GIVEN
        email_idifferent = "zoO@TEst.com"

        # WHEN
        users = self._user_service.get_users(email_idifferent=email_idifferent)
        users_dict = [item.model_dump() for item in users]

        self.save_data_json(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_email_idifferent_wrong(self, expected_data, saved_path):
        # GIVEN
        email_different = "zoO@TEst.c%"

        # WHEN
        users = self._user_service.get_users(email_idifferent=email_different)
        users_dict = [item.model_dump() for item in users]

        self.save_data_json(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_no_load_city(self, expected_data, saved_path):
        # GIVEN
        load_city = False

        # WHEN
        users = self._user_service.get_users(load_city=load_city)
        users_dict = [item.model_dump() for item in users]

        self.save_data_json(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_no_load_adresses(self, expected_data, saved_path):
        # GIVEN
        load_addresses = False

        # WHEN
        users = self._user_service.get_users(load_addresses=load_addresses)
        users_dict = [item.model_dump() for item in users]

        self.save_data_json(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_additional_zip_code_sup(self, expected_data, saved_path):
        # GIVEN
        additional_zip_code_sup = 300

        # WHEN
        users = self._user_service.get_users(
            additional_zip_code_sup=additional_zip_code_sup
        )
        users_dict = [item.model_dump() for item in users]

        self.save_data_json(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )
