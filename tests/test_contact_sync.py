import unittest
from datetime import date

from app.contact_sync import (
    build_contact_payload,
    choose_exact_contact,
    contact_fingerprint,
    extract_source_order_date,
    normalize_tax_id,
    source_is_older,
)


class ContactSyncTests(unittest.TestCase):
    def setUp(self):
        self.cliente = {
            "id": 10,
            "nome": "Maria da Silva",
            "cpfCnpj": "123.456.789-01",
            "email": "MARIA@example.com",
            "telefone": "(11) 99999-8888",
        }
        self.endereco = {
            "endereco": "Rua Um",
            "enderecoNro": "25",
            "bairro": "Centro",
            "municipio": "Sao Paulo",
            "cep": "01001-000",
            "uf": "sp",
        }

    def test_payload_uses_delivery_address_fields(self):
        cliente = {
            **self.cliente,
            "endereco": {"endereco": "Rua Antiga", "numero": "999"},
        }
        payload = build_contact_payload(cliente, self.endereco)
        self.assertEqual(payload["nome"], "Maria da Silva")
        self.assertEqual(payload["tipoPessoa"], "F")
        self.assertEqual(payload["endereco"]["endereco"], "Rua Um")
        self.assertEqual(payload["endereco"]["numero"], "25")
        self.assertEqual(payload["endereco"]["municipio"], "Sao Paulo")

    def test_fingerprint_ignores_formatting_only_changes(self):
        first = build_contact_payload(self.cliente, self.endereco)
        second = build_contact_payload(
            {**self.cliente, "cpfCnpj": "12345678901", "email": "maria@EXAMPLE.com", "telefone": "11999998888"},
            {**self.endereco, "cep": "01001000", "uf": "SP"},
        )
        self.assertEqual(contact_fingerprint(first), contact_fingerprint(second))

    def test_fingerprint_changes_when_address_changes(self):
        first = build_contact_payload(self.cliente, self.endereco)
        second = build_contact_payload(self.cliente, {**self.endereco, "enderecoNro": "26"})
        self.assertNotEqual(contact_fingerprint(first), contact_fingerprint(second))

    def test_older_order_cannot_replace_newer_source(self):
        mapping = {"source_order_date": date(2026, 7, 16), "source_order_a_id": 200}
        self.assertTrue(source_is_older(mapping, date(2026, 7, 15), 250))
        self.assertTrue(source_is_older(mapping, date(2026, 7, 16), 199))
        self.assertFalse(source_is_older(mapping, date(2026, 7, 16), 201))
        self.assertFalse(source_is_older(mapping, date(2026, 7, 17), 100))

    def test_source_order_date_parses_supported_formats(self):
        self.assertEqual(extract_source_order_date({"data": "2026-07-16"}), date(2026, 7, 16))
        self.assertEqual(extract_source_order_date({"dataPedido": "16/07/2026"}), date(2026, 7, 16))

    def test_exact_contact_selection_is_deterministic(self):
        contacts = [
            {"id": 30, "cpfCnpj": "123.456.789-01"},
            {"id": 20, "cpfCnpj": "12345678901"},
            {"id": 10, "cpfCnpj": "99999999999"},
        ]
        match, count = choose_exact_contact(contacts, normalize_tax_id("123.456.789-01"))
        self.assertEqual(match["id"], 20)
        self.assertEqual(count, 2)


if __name__ == "__main__":
    unittest.main()
