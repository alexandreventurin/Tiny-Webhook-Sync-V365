import unittest

from app.order_comparison import normalize_shipping_label, select_delivery_address


class OrderComparisonTests(unittest.TestCase):
    def test_rejuderme_prefix_is_ignored_for_shipping_names(self):
        self.assertEqual(
            normalize_shipping_label("Correios (Sedex)"),
            normalize_shipping_label("Rejuderme - Correios (Sedex)"),
        )
        self.assertEqual(
            normalize_shipping_label("PAC"),
            normalize_shipping_label("REJUDERME: PAC"),
        )

    def test_other_shipping_names_remain_different(self):
        self.assertNotEqual(
            normalize_shipping_label("Correios (Sedex)"),
            normalize_shipping_label("Correios (PAC)"),
        )

    def test_destination_delivery_address_has_priority(self):
        payload = {
            "enderecoEntrega": {"cep": "11111-111", "numero": "10"},
            "cliente": {"endereco": {"cep": "22222-222", "numero": "20"}},
        }
        selected = select_delivery_address(payload, fallback_to_customer=True)
        self.assertEqual(selected["cep"], "11111-111")
        self.assertEqual(selected["numero"], "10")

    def test_customer_address_is_fallback_when_delivery_is_absent(self):
        payload = {
            "enderecoEntrega": {},
            "cliente": {"endereco": {"cep": "22222-222", "enderecoNro": "20"}},
        }
        selected = select_delivery_address(payload, fallback_to_customer=True)
        self.assertEqual(selected["cep"], "22222-222")
        self.assertEqual(selected["enderecoNro"], "20")

    def test_same_fallback_rule_can_be_used_for_origin(self):
        origin = {"cliente": {"endereco": {"cep": "33333-333", "numero": "30"}}}
        selected = select_delivery_address(origin, fallback_to_customer=True)
        self.assertEqual(selected, origin["cliente"]["endereco"])


if __name__ == "__main__":
    unittest.main()
