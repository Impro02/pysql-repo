# LIBS
from pysql_repo.libs.file_lib import save_json_file

# TESTS
from tests._async_base import IsolatedAsyncioTestCase, async_load_expected_data
from tests.models.schemas.user import UserCreate
from tests.utils import SavedPath


class TestUsers(IsolatedAsyncioTestCase):
    @async_load_expected_data(SavedPath.PATH_ASSET_USERS)
    async def test_get_all(self, expected_data, saved_path):
        # WHEN
        users = await self._user_service.get_users()
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @async_load_expected_data(SavedPath.PATH_ASSET_USERS)
    async def test_with_ids_in(self, expected_data, saved_path):
        # GIVEN
        ids_in = [2, 3]

        # WHEN
        users = await self._user_service.get_users(ids_in=ids_in)
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @async_load_expected_data(SavedPath.PATH_ASSET_USERS)
    async def test_with_ids_not_in(self, expected_data, saved_path):
        # GIVEN
        ids_not_in = [2, 3]

        # WHEN
        users = await self._user_service.get_users(ids_not_in=ids_not_in)
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @async_load_expected_data(SavedPath.PATH_ASSET_USERS)
    async def test_with_emails_iin(self, expected_data, saved_path):
        # GIVEN
        emails_iin = ["fOO@TEst.coM", "ZOo@TEST.CoM"]

        # WHEN
        users = await self._user_service.get_users(emails_iin=emails_iin)
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @async_load_expected_data(SavedPath.PATH_ASSET_USERS)
    async def test_with_emails_in(self, expected_data, saved_path):
        # GIVEN
        emails_in = ["foo@test.com", "ZOo@TEST.CoM"]

        # WHEN
        users = await self._user_service.get_users(emails_in=emails_in)
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @async_load_expected_data(SavedPath.PATH_ASSET_USERS)
    async def test_with_emails_not_iin(self, expected_data, saved_path):
        # GIVEN
        emails_not_iin = ["fOO@test.com", "ZOo@TEST.CoM"]

        # WHEN
        users = await self._user_service.get_users(emails_not_iin=emails_not_iin)
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @async_load_expected_data(SavedPath.PATH_ASSET_USERS)
    async def test_with_emails_not_in(self, expected_data, saved_path):
        # GIVEN
        emails_not_in = ["foo@test.com", "ZOo@TEST.CoM"]

        # WHEN
        users = await self._user_service.get_users(emails_not_in=emails_not_in)
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @async_load_expected_data(SavedPath.PATH_ASSET_USERS)
    async def test_with_email_ilike(self, expected_data, saved_path):
        # GIVEN
        email_ilike = "BOo%"

        # WHEN
        users = await self._user_service.get_users(email_ilike=email_ilike)
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @async_load_expected_data(SavedPath.PATH_ASSET_USERS)
    async def test_with_email_like(self, expected_data, saved_path):
        # GIVEN
        email_like = "boo%"

        # WHEN
        users = await self._user_service.get_users(email_like=email_like)
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @async_load_expected_data(SavedPath.PATH_ASSET_USERS)
    async def test_with_email_not_like(self, expected_data, saved_path):
        # GIVEN
        email_not_ilike = "BOo%"

        # WHEN
        users = await self._user_service.get_users(email_not_ilike=email_not_ilike)
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @async_load_expected_data(SavedPath.PATH_ASSET_USERS)
    async def test_with_email_not_like(self, expected_data, saved_path):
        # GIVEN
        email_not_like = "boo%"

        # WHEN
        users = await self._user_service.get_users(email_not_like=email_not_like)
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @async_load_expected_data(SavedPath.PATH_ASSET_USERS)
    async def test_with_email_equal(self, expected_data, saved_path):
        # GIVEN
        email_equal = "zoo@test.com"

        # WHEN
        users = await self._user_service.get_users(email_equal=email_equal)
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @async_load_expected_data(SavedPath.PATH_ASSET_USERS)
    async def test_with_email_equal_wrong(self, expected_data, saved_path):
        # GIVEN
        email_equal = "zoo@test.c%"

        # WHEN
        users = await self._user_service.get_users(email_equal=email_equal)
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @async_load_expected_data(SavedPath.PATH_ASSET_USERS)
    async def test_with_email_iequal_wrong(self, expected_data, saved_path):
        # GIVEN
        email_iequal = "zOO@test.c%"

        # WHEN
        users = await self._user_service.get_users(email_iequal=email_iequal)
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @async_load_expected_data(SavedPath.PATH_ASSET_USERS)
    async def test_with_email_different(self, expected_data, saved_path):
        # GIVEN
        email_different = "zoo@test.com"

        # WHEN
        users = await self._user_service.get_users(email_different=email_different)
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @async_load_expected_data(SavedPath.PATH_ASSET_USERS)
    async def test_with_email_different_wrong(self, expected_data, saved_path):
        # GIVEN
        email_different = "zoo@test.c%"

        # WHEN
        users = await self._user_service.get_users(email_different=email_different)
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @async_load_expected_data(SavedPath.PATH_ASSET_USERS)
    async def test_with_email_idifferent(self, expected_data, saved_path):
        # GIVEN
        email_idifferent = "zoO@TEst.com"

        # WHEN
        users = await self._user_service.get_users(email_idifferent=email_idifferent)
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @async_load_expected_data(SavedPath.PATH_ASSET_USERS)
    async def test_with_email_idifferent_wrong(self, expected_data, saved_path):
        # GIVEN
        email_different = "zoO@TEst.c%"

        # WHEN
        users = await self._user_service.get_users(email_idifferent=email_different)
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @async_load_expected_data(SavedPath.PATH_ASSET_USERS)
    async def test_no_load_city(self, expected_data, saved_path):
        # GIVEN
        load_city = False

        # WHEN
        users = await self._user_service.get_users(load_city=load_city)
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @async_load_expected_data(SavedPath.PATH_ASSET_USERS)
    async def test_no_load_addresses(self, expected_data, saved_path):
        # GIVEN
        load_addresses = False

        # WHEN
        users = await self._user_service.get_users(load_addresses=load_addresses)
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @async_load_expected_data(SavedPath.PATH_ASSET_USERS)
    async def test_with_zip_codes_not_in(self, expected_data, saved_path):
        # GIVEN
        zip_codes_not_in = [121]

        # WHEN
        users = await self._user_service.get_users(zip_codes_not_in=zip_codes_not_in)
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )


class TestUsersPaginate(IsolatedAsyncioTestCase):
    @async_load_expected_data(SavedPath.PATH_ASSET_USERS)
    async def test_with_zip_codes_not_in(self, expected_data, saved_path):
        # GIVEN
        expected_pagination = '{"total": 2, "page": 1, "per_page": 2, "total_pages": 1}'

        zip_codes_not_in = [290]
        page = 1
        per_page = 2

        # WHEN
        users, paginate = await self._user_service.get_users_paginate(
            page=page,
            per_page=per_page,
            zip_codes_not_in=zip_codes_not_in,
        )
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_pagination,
            paginate,
        )
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @async_load_expected_data(SavedPath.PATH_ASSET_USERS)
    async def test_with_zip_codes_in(self, expected_data, saved_path):
        # GIVEN
        expected_pagination = '{"total": 3, "page": 1, "per_page": 2, "total_pages": 2}'

        zip_codes_in = [9898, 876, 290]
        page = 1
        per_page = 2

        # WHEN
        users, paginate = await self._user_service.get_users_paginate(
            page=page,
            per_page=per_page,
            zip_codes_in=zip_codes_in,
        )
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_pagination,
            paginate,
        )
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @async_load_expected_data(SavedPath.PATH_ASSET_USERS)
    async def test_with_zip_codes_in(self, expected_data, saved_path):
        # GIVEN
        expected_pagination = '{"total": 3, "page": 1, "per_page": 2, "total_pages": 2}'

        zip_codes_in = [9898, 876, 290]
        page = 1
        per_page = 2

        # WHEN
        users, paginate = await self._user_service.get_users_paginate(
            page=page,
            per_page=per_page,
            zip_codes_in=zip_codes_in,
        )
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_pagination,
            paginate,
        )
        self.assertEqual(
            expected_data,
            users_dict,
        )


class TestCreateUser(IsolatedAsyncioTestCase):
    @async_load_expected_data(SavedPath.PATH_ASSET_USERS)
    async def test_create(self, expected_data, saved_path):
        # GIVEN
        payload = UserCreate(
            email="zoo@demo.com",
            hashed_password="zoo",
            full_name="Zoo Boo",
        )

        # WHEN
        users = await self._user_service.create_user(
            data=payload,
        )
        user_dict = users.model_dump()

        save_json_file(saved_path, user_dict)

        # THEN
        self.assertEqual(
            expected_data,
            user_dict,
        )


class TestCreateUsers(IsolatedAsyncioTestCase):
    @async_load_expected_data(SavedPath.PATH_ASSET_USERS)
    async def test_create(self, expected_data, saved_path):
        # GIVEN
        payload = [
            UserCreate(
                email="zoo@demo.com",
                hashed_password="zoo",
                full_name="Zoo Boo",
            ),
            UserCreate(
                email="too@demo.com",
                hashed_password="uii",
                full_name="Koo Moo",
            ),
        ]

        # WHEN
        users = await self._user_service.create_users(
            data=payload,
        )
        users_dict = [user.model_dump() for user in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )


class TestPathUser(IsolatedAsyncioTestCase):
    @async_load_expected_data(SavedPath.PATH_ASSET_USERS)
    async def test_path_email(self, expected_data, saved_path):
        # GIVEN
        email = "zoo@doo.com"

        # WHEN
        user = await self._user_service.patch_email(id=2, email=email)
        user_dict = user.model_dump()

        save_json_file(saved_path, user_dict)

        # THEN
        self.assertEqual(
            expected_data,
            user_dict,
        )


class TestPathUsers(IsolatedAsyncioTestCase):
    @async_load_expected_data(SavedPath.PATH_ASSET_USERS)
    async def test_path_disable(self, expected_data, saved_path):
        # GIVEN
        ids = [1, 3]

        # WHEN
        users = await self._user_service.patch_disable(ids=ids)
        users_dict = [user.model_dump() for user in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )


class TestDeleteUser(IsolatedAsyncioTestCase):
    async def test_delete(self):
        # GIVEN
        id = 1

        # WHEN
        is_deleted = await self._user_service.delete_by_id(id=id)

        # THEN
        self.assertEqual(
            True,
            is_deleted,
        )


class TestDeleteUsers(IsolatedAsyncioTestCase):
    async def test_delete_all(self):
        # GIVEN
        ids = [1, 3]

        # WHEN
        is_deleted = await self._user_service.delete_by_ids(ids=ids)

        # THEN
        self.assertEqual(
            True,
            is_deleted,
        )
