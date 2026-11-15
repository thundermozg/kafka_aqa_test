def test_receipt_preserves_amount(receipt_scenario):
    payment, receipt = receipt_scenario

    assert receipt.amount == payment.amount