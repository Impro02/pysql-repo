# MODULES
from pathlib import Path
from typing import Any, Dict, List, Union

# LIBS
from pysql_repo.libs.file_lib import save_json_file

# TESTS
from tests.models.schemas.user import UserCreate

# TESTS
from tests.utils import SavedPath
from tests._base import TestCase, load_expected_data


class TestUsers(TestCase):
    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_get_all(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
        # WHEN
        with self._database.session_factory() as session:
            users = self._user_service.get_users(
                session,
            )
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_ids_in(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
        # GIVEN
        ids_in = [2, 3]

        # WHEN
        with self._database.session_factory() as session:
            users = self._user_service.get_users(
                session,
                ids_in=ids_in,
            )
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_ids_not_in(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
        # GIVEN
        ids_not_in = [2, 3]

        # WHEN
        with self._database.session_factory() as session:
            users = self._user_service.get_users(
                session,
                ids_not_in=ids_not_in,
            )
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_emails_iin(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
        # GIVEN
        emails_iin = ["fOO@TEst.coM", "ZOo@TEST.CoM"]

        # WHEN
        with self._database.session_factory() as session:
            users = self._user_service.get_users(
                session,
                emails_iin=emails_iin,
            )
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_emails_in(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
        # GIVEN
        emails_in = ["foo@test.com", "ZOo@TEST.CoM"]

        # WHEN
        with self._database.session_factory() as session:
            users = self._user_service.get_users(
                session,
                emails_in=emails_in,
            )
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_emails_not_iin(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
        # GIVEN
        emails_not_iin = ["fOO@test.com", "ZOo@TEST.CoM"]

        # WHEN
        with self._database.session_factory() as session:
            users = self._user_service.get_users(
                session,
                emails_not_iin=emails_not_iin,
            )
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_emails_not_in(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
        # GIVEN
        emails_not_in = ["foo@test.com", "ZOo@TEST.CoM"]

        # WHEN
        with self._database.session_factory() as session:
            users = self._user_service.get_users(
                session,
                emails_not_in=emails_not_in,
            )
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_email_ilike(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
        # GIVEN
        email_ilike = "BOo%"

        # WHEN
        with self._database.session_factory() as session:
            users = self._user_service.get_users(
                session,
                email_ilike=email_ilike,
            )
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_email_like(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
        # GIVEN
        email_like = "boo%"

        # WHEN
        with self._database.session_factory() as session:
            users = self._user_service.get_users(
                session,
                email_like=email_like,
            )
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_email_not_ilike(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
        # GIVEN
        email_not_ilike = "BOo%"

        # WHEN
        with self._database.session_factory() as session:
            users = self._user_service.get_users(
                session,
                email_not_ilike=email_not_ilike,
            )
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_email_not_like(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
        # GIVEN
        email_not_like = "boo%"

        # WHEN
        with self._database.session_factory() as session:
            users = self._user_service.get_users(
                session,
                email_not_like=email_not_like,
            )
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_email_equal(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
        # GIVEN
        email_equal = "zoo@test.com"

        # WHEN
        with self._database.session_factory() as session:
            users = self._user_service.get_users(
                session,
                email_equal=email_equal,
            )
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_email_equal_wrong(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
        # GIVEN
        email_equal = "zoo@test.c%"

        # WHEN
        with self._database.session_factory() as session:
            users = self._user_service.get_users(
                session,
                email_equal=email_equal,
            )
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_email_iequal_wrong(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
        # GIVEN
        email_iequal = "zOO@test.c%"

        # WHEN
        with self._database.session_factory() as session:
            users = self._user_service.get_users(
                session,
                email_iequal=email_iequal,
            )
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_email_different(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
        # GIVEN
        email_different = "zoo@test.com"

        # WHEN
        with self._database.session_factory() as session:
            users = self._user_service.get_users(
                session,
                email_different=email_different,
            )
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_email_different_wrong(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
        # GIVEN
        email_different = "zoo@test.c%"

        # WHEN
        with self._database.session_factory() as session:
            users = self._user_service.get_users(
                session,
                email_different=email_different,
            )
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_email_idifferent(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
        # GIVEN
        email_idifferent = "zoO@TEst.com"

        # WHEN
        with self._database.session_factory() as session:
            users = self._user_service.get_users(
                session,
                email_idifferent=email_idifferent,
            )
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_email_idifferent_wrong(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
        # GIVEN
        email_different = "zoO@TEst.c%"

        # WHEN
        with self._database.session_factory() as session:
            users = self._user_service.get_users(
                session,
                email_idifferent=email_different,
            )
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_no_load_city(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
        # GIVEN
        load_city = False

        # WHEN
        with self._database.session_factory() as session:
            users = self._user_service.get_users(
                session,
                load_city=load_city,
            )
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_no_load_addresses(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
        # GIVEN
        load_addresses = False

        # WHEN
        with self._database.session_factory() as session:
            users = self._user_service.get_users(
                session,
                load_addresses=load_addresses,
            )
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_zip_codes_not_in(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
        # GIVEN
        zip_codes_not_in = [121]

        # WHEN
        with self._database.session_factory() as session:
            users = self._user_service.get_users(
                session,
                zip_codes_not_in=zip_codes_not_in,
            )
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_zip_codes_in(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
        # GIVEN
        zip_codes_in = [9898, 876, 290]

        # WHEN
        with self._database.session_factory() as session:
            users = self._user_service.get_users(
                session,
                zip_codes_in=zip_codes_in,
            )
        users_dict = [item.model_dump() for item in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )


class TestUsersPaginate(TestCase):
    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_zip_codes_not_in(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
        # GIVEN
        expected_pagination = '{"total": 2, "page": 1, "per_page": 2, "total_pages": 1}'

        zip_codes_not_in = [290]
        page = 1
        per_page = 2

        # WHEN
        with self._database.session_factory() as session:
            users, paginate = self._user_service.get_users_paginate(
                session,
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

    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_with_zip_codes_in(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
        # GIVEN
        expected_pagination = '{"total": 3, "page": 1, "per_page": 2, "total_pages": 2}'

        zip_codes_in = [9898, 876, 290]
        page = 1
        per_page = 2

        # WHEN
        with self._database.session_factory() as session:
            users, paginate = self._user_service.get_users_paginate(
                session,
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


class TestCreateUser(TestCase):
    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_create(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
        # GIVEN
        payload = UserCreate(
            email="zoo@demo.com",
            hashed_password="zoo",
            full_name="Zoo Boo",
        )

        # WHEN
        with self._database.session_factory() as session:
            users = self._user_service.create_user(
                session,
                data=payload,
            )
        user_dict = users.model_dump()

        save_json_file(saved_path, user_dict)

        # THEN
        self.assertEqual(
            expected_data,
            user_dict,
        )


class TestCreateUsers(TestCase):
    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_create(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
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
        with self._database.session_factory() as session:
            users = self._user_service.create_users(
                session,
                data=payload,
            )
        users_dict = [user.model_dump() for user in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )


class TestPatchUser(TestCase):
    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_patch_email(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
        # GIVEN
        email = "zoo@doo.com"

        # WHEN
        with self._database.session_factory() as session:
            user = self._user_service.patch_email(
                session,
                id=2,
                email=email,
            )
        user_dict = user.model_dump()

        save_json_file(saved_path, user_dict)

        # THEN
        self.assertEqual(
            expected_data,
            user_dict,
        )


class TestBulkPatchUsers(TestCase):
    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_bulk_patch_email(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
        # GIVEN
        data = [
            (1, "foo@test.com"),
            (3, "bar@test.com"),
        ]

        # WHEN
        with self._database.session_factory() as session:
            users = self._user_service.bulk_patch_email(
                session,
                data=data,
            )
        users_dict = [user.model_dump() for user in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )


class TestPatchUsers(TestCase):
    @load_expected_data(SavedPath.PATH_ASSET_USERS)
    def test_patch_disable(
        self,
        expected_data: Union[Dict[str, Any], List[Dict[str, Any]]],
        saved_path: Path,
    ) -> None:
        # GIVEN
        ids = [1, 3]

        # WHEN
        with self._database.session_factory() as session:
            users = self._user_service.patch_disable(
                session,
                ids=ids,
            )
        users_dict = [user.model_dump() for user in users]

        save_json_file(saved_path, users_dict)

        # THEN
        self.assertEqual(
            expected_data,
            users_dict,
        )


class TestDeleteUser(TestCase):
    def test_delete(self) -> None:
        # GIVEN
        id = 1

        # WHEN
        with self._database.session_factory() as session:
            is_deleted = self._user_service.delete_by_id(
                session,
                id=id,
            )

        # THEN
        self.assertEqual(
            True,
            is_deleted,
        )

    def test_delete_not_found(self) -> None:
        # GIVEN
        id = -1

        # WHEN
        with self._database.session_factory() as session:
            is_deleted = self._user_service.delete_by_id(
                session,
                id=id,
            )

        # THEN
        self.assertEqual(
            False,
            is_deleted,
        )


class TestDeleteUsers(TestCase):
    def test_delete_all(self) -> None:
        # GIVEN
        ids = [1, 3]

        # WHEN
        with self._database.session_factory() as session:
            is_deleted = self._user_service.delete_by_ids(
                session,
                ids=ids,
            )

        # THEN
        self.assertEqual(
            True,
            is_deleted,
        )

    def test_delete_all_not_found(self) -> None:
        # GIVEN
        ids = [-1, -3]

        # WHEN
        with self._database.session_factory() as session:
            is_deleted = self._user_service.delete_by_ids(
                session,
                ids=ids,
            )

        # THEN
        self.assertEqual(
            False,
            is_deleted,
        )
