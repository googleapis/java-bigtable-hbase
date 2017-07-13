package com.google.cloud.bigtable.hbase.suites;

import com.google.cloud.bigtable.hbase.IntegrationTests;
import com.google.cloud.bigtable.hbase.KnownGap;
import com.google.cloud.bigtable.hbase.categories.KnownEmulatorGap;
import org.junit.experimental.categories.Categories;
import org.junit.experimental.categories.Categories.ExcludeCategory;
import org.junit.runner.RunWith;

@RunWith(Categories.class)
@ExcludeCategory({KnownGap.class, KnownEmulatorGap.class})
public class EmulatorTests extends IntegrationTests { }
