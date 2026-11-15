from services.receipts.kafka.models.receipt import ReceiptStatus

def test_receipt_is_paid(receipt_scenario):
    _, receipt = receipt_scenario

    assert receipt.status == ReceiptStatus.PAID