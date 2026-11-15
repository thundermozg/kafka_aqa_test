def test_receipt_preserves_description(receipt_scenario):
    payment, receipt = receipt_scenario

    assert receipt.description == payment.description