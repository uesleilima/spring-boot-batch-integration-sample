package com.uesleilima.spring.batch.integration.domain;

import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.stereotype.Repository;

/**
 * @author Ueslei Lima
 *
 */
@Repository
public interface EntryRepository extends PagingAndSortingRepository<Entry, String> {

}
