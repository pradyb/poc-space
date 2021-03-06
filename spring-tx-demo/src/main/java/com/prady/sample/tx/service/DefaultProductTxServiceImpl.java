/**
 *
 */
package com.prady.sample.tx.service;

import javax.transaction.Transactional;

import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import com.prady.sample.tx.dto.ProductDTO;

/**
 * @author Prady
 *
 */
@Primary
@Service
public class DefaultProductTxServiceImpl extends AbstractProductTxServiceImpl {

    @Override
    @Transactional
    public ProductDTO save(ProductDTO productDTO) {
        return super.save(productDTO);
    }
}
