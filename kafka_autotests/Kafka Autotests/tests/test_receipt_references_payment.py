

def test_receipt_references_payment(receipt_scenario):
    payment, receipt = receipt_scenario

    assert receipt.receipt_id == payment.id