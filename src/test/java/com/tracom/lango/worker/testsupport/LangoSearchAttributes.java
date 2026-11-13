package com.tracom.lango.worker.testsupport;

import io.temporal.api.enums.v1.IndexedValueType;
import io.temporal.testing.TestWorkflowEnvironment;

/**
 * Temporal rejects upserts of search attributes it doesn't know, so they must
 * be registered first. These are the ones the Go backend and the workflow use.
 * (On a real cluster: `temporal operator search-attribute create ...`, see README.)
 */
public final class LangoSearchAttributes {

    private LangoSearchAttributes() {
    }

    public static void register(TestWorkflowEnvironment env) {
        env.registerSearchAttribute("LangoRegion", IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD);
        env.registerSearchAttribute("LangoBranch", IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD);
        env.registerSearchAttribute("LangoTicketType", IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD);
        env.registerSearchAttribute("LangoTicketStatus", IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD);
        env.registerSearchAttribute("LangoSlaStatus", IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD);
        env.registerSearchAttribute("LangoSlaDate", IndexedValueType.INDEXED_VALUE_TYPE_DATETIME);
        env.registerSearchAttribute("LangoEscalationTier", IndexedValueType.INDEXED_VALUE_TYPE_INT);
    }
}
