package com.google.cloud.bigtable.hbase;

import org.junit.experimental.categories.Categories;
import org.junit.experimental.categories.Categories.IncludeCategory;
import org.junit.runner.RunWith;


@RunWith(Categories.class)
@IncludeCategory(KnownGap.class)
class IntegrationTestsGap extends IntegrationTests {
}
