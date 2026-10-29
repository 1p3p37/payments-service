from typing import Annotated
from uuid import UUID

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter, Depends, Header, status

from app.api.auth import verify_api_key
from app.api.schemas import (
    CreatePaymentRequest,
    CreatePaymentResponse,
    ErrorResponse,
    PaymentResponse,
)
from app.application.commands.create_payment import CreatePaymentCommand
from app.application.mediator import Mediator
from app.application.queries.get_payment import GetPaymentQuery
from app.infra.uow import UnitOfWork

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    route_class=DishkaRoute,
    dependencies=[Depends(verify_api_key)],
    responses={401: {"model": ErrorResponse}},
)


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    responses={409: {"model": ErrorResponse, "description": "Idempotency-Key reused"}},
)
async def create_payment(
    body: CreatePaymentRequest,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)],
    uow: FromDishka[UnitOfWork],
    mediator: FromDishka[Mediator],
) -> CreatePaymentResponse:
    """Принять платёж в обработку. Повтор с тем же ключом и телом возвращает тот же ответ."""
    async with uow:
        payment = await mediator.send(
            CreatePaymentCommand(
                idempotency_key=idempotency_key,
                amount=body.amount,
                currency=body.currency,
                description=body.description,
                metadata=body.metadata,
                webhook_url=str(body.webhook_url),
            )
        )
    return CreatePaymentResponse.from_entity(payment)


@router.get("/{payment_id}", responses={404: {"model": ErrorResponse}})
async def get_payment(
    payment_id: UUID,
    uow: FromDishka[UnitOfWork],
    mediator: FromDishka[Mediator],
) -> PaymentResponse:
    async with uow:
        payment = await mediator.query(GetPaymentQuery(payment_id))
    return PaymentResponse.from_entity(payment)
