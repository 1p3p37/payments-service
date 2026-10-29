from app.infra.models import PaymentModel
from tests.factories import make_payment


def test_payment_to_dict_matches_table_columns():
    # PaymentCommandRepository.add вставляет to_dict() напрямую в таблицу
    assert set(make_payment().to_dict()) == set(PaymentModel.__table__.c.keys())


def test_to_entity_roundtrip():
    payment = make_payment()
    data = payment.to_dict()
    data["metadata_"] = data.pop("metadata")
    model = PaymentModel(**data)

    assert model.to_entity().to_dict() == payment.to_dict()
