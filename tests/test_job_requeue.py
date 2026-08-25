import unittest

from app.utils import is_approved_create_request


class JobRequeueTests(unittest.TestCase):
    def test_approved_create_request_can_revive_skipped_job(self):
        self.assertTrue(
            is_approved_create_request(
                "create_order_c",
                {"codigo_situacao": "aprovado"},
            )
        )
        self.assertTrue(
            is_approved_create_request(
                "create_order_c",
                {"codigo_situacao": 3},
            )
        )

    def test_open_or_unrelated_jobs_cannot_revive_completed_create(self):
        self.assertFalse(
            is_approved_create_request(
                "create_order_c",
                {"codigo_situacao": "em_aberto"},
            )
        )
        self.assertFalse(
            is_approved_create_request(
                "sync_status",
                {"codigo_situacao": "aprovado"},
            )
        )


if __name__ == "__main__":
    unittest.main()
