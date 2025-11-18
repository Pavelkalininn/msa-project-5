package com.example.batchprocessing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.DataClassRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicReference;

@Component
public class ProductItemProcessor implements ItemProcessor<Product, Product> {

	private static final Logger log = LoggerFactory.getLogger(ProductItemProcessor.class);

	@Autowired
	private JdbcTemplate jdbcTemplate;

    @Override
	public Product process(final Product product) {
    log.info("Processing product: {}", product.productName());

    if (product.productName() == null || product.productName().trim().isEmpty()) {
        log.warn("Product with SKU {} has empty name, skipping", product.productSku());
        return null;
    }

    if (product.productAmount() <= 0) {
        log.warn("Product {} has invalid amount: {}, setting to 0",
                product.productName(), product.productAmount());
        return new Product(
            product.productId(),
            product.productSku(),
            product.productName().trim().toUpperCase(),
            0L,
            product.productData()
        );
    }

    String loyaltyQuery = "SELECT loyalityData FROM loyality_data WHERE productSku = ?";
    String loyaltyData = null;
    try {
        loyaltyData = jdbcTemplate.queryForObject(loyaltyQuery, String.class, product.productSku());
    } catch (Exception e) {
        log.debug("No loyalty data found for product SKU: {}", product.productSku());
    }

    final String enrichedProductData;
    if (loyaltyData != null) {
        enrichedProductData = product.productData() + " | Loyalty: " + loyaltyData;
    } else {
        enrichedProductData = product.productData();
    }

    final String normalizedName = product.productName().trim().toUpperCase();

    final Product processedProduct = new Product(
        product.productId(),
        product.productSku(),
        normalizedName,
        product.productAmount(),
        enrichedProductData
    );

    log.info("Processed '{}' -> '{}'", product.productName(), normalizedName);

    return processedProduct;
}

}
