def test_receipt_preserves_currency(receipt_scenario):
    payment, receipt = receipt_scenario

    assert receipt.currency == payment.currency