package com.google.cloud.bigtable.hbase;

import org.junit.experimental.categories.Categories;
import org.junit.experimental.categories.Categories.ExcludeCategory;
import org.junit.runner.RunWith;


@RunWith(Categories.class)
@ExcludeCategory(KnownGap.class)
class IntegrationTestsNoKnownGap extends IntegrationTests {
}
