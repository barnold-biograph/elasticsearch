/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.BytesRefHashTable;
import org.elasticsearch.compute.aggregation.blockhash.HashImplFactory;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

/**
 * Operator that tracks the distinct values of a single key column across pages.
 * It either filters repeated keys or acts as a pass-through uniqueness guard.
 * Null key positions are never treated as duplicates.
 */
public final class DistinctByOperator extends AbstractPageMappingOperator {

    /** Builds a {@link DistinctByOperator} for a {@code BYTES_REF} key. */
    public record BytesRefFactory(int keyChannel, boolean failOnDuplicate) implements OperatorFactory {

        public BytesRefFactory(int keyChannel) {
            this(keyChannel, false);
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new DistinctByOperator(
                keyChannel(),
                new BytesRefProcessor(keyChannel(), OnDuplicateKeyPolicy.newPolicy(failOnDuplicate), driverContext.blockFactory())
            );
        }

        @Override
        public String describe() {
            return "DistinctByOperator[keyChannel=" + keyChannel + ", type=BYTES_REF]";
        }
    }

    /** Make {@link DistinctByOperator} for compact, non-negative, integer ordinal type. */
    public record OrdinalFactory(int keyChannel, boolean failOnDuplicate) implements OperatorFactory {

        public OrdinalFactory(int keyChannel) {
            this(keyChannel, false);
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new DistinctByOperator(
                keyChannel(),
                new OrdinalIntegerProcessor(keyChannel(), OnDuplicateKeyPolicy.newPolicy(failOnDuplicate), driverContext.bigArrays())
            );
        }

        @Override
        public String describe() {
            return "DistinctByOperator[keyChannel=" + keyChannel + ", type=ORDINAL]";
        }
    }

    private final int keyChannel;
    private final Processor processor;

    private DistinctByOperator(int keyChannel, Processor processor) {
        this.keyChannel = keyChannel;
        this.processor = processor;
    }

    @Override
    protected Page process(Page page) {
        return processor.process(page);
    }

    @Override
    public String toString() {
        return "DistinctByOperator[keyChannel=" + keyChannel + ", " + processor + "]";
    }

    @Override
    public void close() {
        Releasables.close(processor, super::close);
    }

    private abstract static class Processor implements Releasable {
        protected final int keyChannel;
        protected final OnDuplicateKeyPolicy onDuplicateKeyPolicy;

        private Processor(int keyChannel, OnDuplicateKeyPolicy onDuplicateKeyPolicy) {
            this.keyChannel = keyChannel;
            this.onDuplicateKeyPolicy = onDuplicateKeyPolicy;
        }

        /** Processes the key block and reports each position to {@link #onDuplicateKeyPolicy}. */
        protected abstract void processKeys(Page page);

        final Page process(Page page) {
            onDuplicateKeyPolicy.beginPage(page.getPositionCount());
            processKeys(page);
            return onDuplicateKeyPolicy.finishPage(page);
        }

        @Override
        public void close() {}
    }

    /** Policy mixed into a {@link Processor} to select or reject duplicate keys. */
    private abstract static class OnDuplicateKeyPolicy {
        private final boolean allValues;
        private final String name;

        static OnDuplicateKeyPolicy newPolicy(boolean failOnDuplicate) {
            if (failOnDuplicate) {
                return new FailOnDuplicateKeyPolicy();
            }
            return new SkipOnDuplicateKeyPolicy();
        }

        private OnDuplicateKeyPolicy(boolean allValues, String name) {
            this.allValues = allValues;
            this.name = name;
        }

        void beginPage(int positionCount) {}

        final void constant(int positionCount, boolean duplicate) {
            if (positionCount > 0) {
                key(0, duplicate);
            }
            if (positionCount > 1) {
                key(1, true);
            }
        }

        /** Handles a non-null key at {@code position}. */
        abstract void key(int position, boolean duplicate);

        void nullKey(int position) {}

        Page finishPage(Page page) {
            return page;
        }
    }

    private static final class SkipOnDuplicateKeyPolicy extends OnDuplicateKeyPolicy {
        private int[] selectedPositions = new int[0];
        private int[] positions;
        private int positionCount;
        private int selectedCount;

        private SkipOnDuplicateKeyPolicy() {
            super(false, "Dedup");
        }

        @Override
        void beginPage(int positionCount) {
            this.positionCount = positionCount;
            positions = null;
            selectedCount = 0;
        }

        @Override
        void key(int position, boolean duplicate) {
            if (duplicate) {
                reject(position);
            } else if (positions != null) {
                positions[selectedCount++] = position;
            }
        }

        @Override
        void nullKey(int position) {
            reject(position);
        }

        @Override
        Page finishPage(Page page) {
            Page result;
            if (positions == null) {
                result = page.shallowCopy();
            } else if (selectedCount == 0) {
                result = null;
            } else {
                result = page.filter(false, positions, 0, selectedCount);
            }
            page.releaseBlocks();
            return result;
        }

        private void reject(int rejectedPosition) {
            if (positions == null) {
                positions = beginSelection(positionCount, rejectedPosition);
                selectedCount = rejectedPosition;
            }
        }

        private int[] beginSelection(int positionCount, int rejectedPosition) {
            if (selectedPositions.length < positionCount) {
                selectedPositions = new int[positionCount];
            }
            for (int p = 0; p < rejectedPosition; p++) {
                selectedPositions[p] = p;
            }
            return selectedPositions;
        }
    }

    private static final class FailOnDuplicateKeyPolicy extends OnDuplicateKeyPolicy {
        private FailOnDuplicateKeyPolicy() {
            super(true, "Guard");
        }

        @Override
        void key(int position, boolean duplicate) {
            if (duplicate) {
                throw new IllegalArgumentException("input channel cannot emit duplicate keys when [failOnDuplicate] is [true]");
            }
        }
    }

    private static final class BytesRefProcessor extends Processor {
        private final BytesRefHashTable seenKeys;
        private final BytesRef scratch = new BytesRef();
        private byte[] pageOrdinalGenerations = new byte[0];
        private byte pageGeneration;

        private BytesRefProcessor(int keyChannel, OnDuplicateKeyPolicy onDuplicateKeyPolicy, BlockFactory blockFactory) {
            super(keyChannel, onDuplicateKeyPolicy);
            this.seenKeys = HashImplFactory.newBytesRefHash(blockFactory);
        }

        @Override
        public void close() {
            seenKeys.close();
        }

        private byte beginOrdinalPage(int dictionarySize) {
            byte generation = (byte) (pageGeneration + 1);
            if (generation == 0) {
                pageOrdinalGenerations = new byte[dictionarySize];
                generation = 1;
            } else if (pageOrdinalGenerations.length < dictionarySize) {
                pageOrdinalGenerations = new byte[dictionarySize];
            }
            pageGeneration = generation;
            return generation;
        }

        /** Returns {@code true} only for the first occurrence of {@code ordinal} in the current page. */
        private boolean markFirstPageOrdinal(int ordinal, byte generation) {
            if (pageOrdinalGenerations[ordinal] == generation) {
                return false;
            }
            pageOrdinalGenerations[ordinal] = generation;
            return true;
        }

        @Override
        protected void processKeys(Page page) {
            BytesRefBlock keyBlock = page.getBlock(keyChannel);
            BytesRefVector vector = keyBlock.asVector();
            if (vector != null) {
                if (vector.isConstant()) {
                    processConstantVector(vector);
                    return;
                }
                OrdinalBytesRefVector ordinals = vector.asOrdinals();
                if (ordinals == null) {
                    processVector(vector);
                } else {
                    processOrdinalsVector(ordinals);
                }
                return;
            }

            OrdinalBytesRefBlock ordinals = keyBlock.asOrdinals();
            if (ordinals == null) {
                if (onDuplicateKeyPolicy.allValues) {
                    processAllValues(keyBlock);
                } else {
                    processFirstValue(keyBlock);
                }
            } else if (onDuplicateKeyPolicy.allValues) {
                processAllOrdinalValues(ordinals);
            } else {
                processFirstOrdinalValue(ordinals);
            }
        }

        private void processConstantVector(BytesRefVector vector) {
            onDuplicateKeyPolicy.constant(vector.getPositionCount(), seenKeys.add(vector.getBytesRef(0, scratch)) < 0);
        }

        private void processVector(BytesRefVector vector) {
            int positionCount = vector.getPositionCount();
            for (int p = 0; p < positionCount; p++) {
                onDuplicateKeyPolicy.key(p, seenKeys.add(vector.getBytesRef(p, scratch)) < 0);
            }
        }

        private void processFirstValue(BytesRefBlock block) {
            int positionCount = block.getPositionCount();
            for (int p = 0; p < positionCount; p++) {
                if (block.isNull(p)) {
                    onDuplicateKeyPolicy.nullKey(p);
                } else {
                    onDuplicateKeyPolicy.key(p, seenKeys.add(block.getBytesRef(block.getFirstValueIndex(p), scratch)) < 0);
                }
            }
        }

        private void processAllValues(BytesRefBlock block) {
            int positionCount = block.getPositionCount();
            for (int p = 0; p < positionCount; p++) {
                int first = block.getFirstValueIndex(p);
                int end = first + block.getValueCount(p);
                for (int valueIndex = first; valueIndex < end; valueIndex++) {
                    onDuplicateKeyPolicy.key(p, seenKeys.add(block.getBytesRef(valueIndex, scratch)) < 0);
                }
            }
        }

        private void processOrdinalsVector(OrdinalBytesRefVector ordinals) {
            IntVector ordinalVector = ordinals.getOrdinalsVector();
            BytesRefVector dictionary = ordinals.getDictionaryVector();
            byte generation = beginOrdinalPage(dictionary.getPositionCount());
            int positionCount = ordinalVector.getPositionCount();

            for (int p = 0; p < positionCount; p++) {
                processOrdinal(p, dictionary, ordinalVector.getInt(p), generation);
            }
        }

        private void processFirstOrdinalValue(OrdinalBytesRefBlock ordinals) {
            IntBlock ordinalBlock = ordinals.getOrdinalsBlock();
            BytesRefVector dictionary = ordinals.getDictionaryVector();
            byte generation = beginOrdinalPage(dictionary.getPositionCount());
            int positionCount = ordinalBlock.getPositionCount();

            for (int p = 0; p < positionCount; p++) {
                if (ordinalBlock.isNull(p)) {
                    onDuplicateKeyPolicy.nullKey(p);
                } else {
                    processOrdinal(p, dictionary, ordinalBlock.getInt(ordinalBlock.getFirstValueIndex(p)), generation);
                }
            }
        }

        private void processAllOrdinalValues(OrdinalBytesRefBlock ordinals) {
            IntBlock ordinalBlock = ordinals.getOrdinalsBlock();
            BytesRefVector dictionary = ordinals.getDictionaryVector();
            byte generation = beginOrdinalPage(dictionary.getPositionCount());
            int positionCount = ordinalBlock.getPositionCount();

            for (int p = 0; p < positionCount; p++) {
                int first = ordinalBlock.getFirstValueIndex(p);
                int end = first + ordinalBlock.getValueCount(p);
                for (int valueIndex = first; valueIndex < end; valueIndex++) {
                    processOrdinal(p, dictionary, ordinalBlock.getInt(valueIndex), generation);
                }
            }
        }

        private void processOrdinal(int position, BytesRefVector dictionary, int ordinal, byte generation) {
            boolean duplicate = markFirstPageOrdinal(ordinal, generation) == false;
            if (duplicate == false) {
                duplicate = seenKeys.add(dictionary.getBytesRef(ordinal, scratch)) < 0;
            }
            onDuplicateKeyPolicy.key(position, duplicate);
        }

        @Override
        public String toString() {
            return "BytesRef" + onDuplicateKeyPolicy.name + "Processor[seenKeys=" + seenKeys.size() + "]";
        }
    }

    private static final class OrdinalIntegerProcessor extends Processor {
        private final BitArray seen;

        private OrdinalIntegerProcessor(int keyChannel, OnDuplicateKeyPolicy onDuplicateKeyPolicy, BigArrays bigArrays) {
            super(keyChannel, onDuplicateKeyPolicy);
            this.seen = new BitArray(1, bigArrays);
        }

        @Override
        public void close() {
            seen.close();
        }

        @Override
        protected void processKeys(Page page) {
            IntBlock ordinalBlock = page.getBlock(keyChannel);
            IntVector vector = ordinalBlock.asVector();
            if (vector == null) {
                if (onDuplicateKeyPolicy.allValues) {
                    processAllValues(ordinalBlock);
                } else {
                    processFirstValue(ordinalBlock);
                }
            } else if (vector.isConstant()) {
                onDuplicateKeyPolicy.constant(vector.getPositionCount(), getAndSetOrdinal(vector.getInt(0)));
            } else {
                processVector(vector);
            }
        }

        private void processVector(IntVector vector) {
            int positionCount = vector.getPositionCount();
            for (int p = 0; p < positionCount; p++) {
                onDuplicateKeyPolicy.key(p, getAndSetOrdinal(vector.getInt(p)));
            }
        }

        private void processFirstValue(IntBlock block) {
            int positionCount = block.getPositionCount();
            for (int p = 0; p < positionCount; p++) {
                if (block.isNull(p)) {
                    onDuplicateKeyPolicy.nullKey(p);
                } else {
                    onDuplicateKeyPolicy.key(p, getAndSetOrdinal(block.getInt(block.getFirstValueIndex(p))));
                }
            }
        }

        private void processAllValues(IntBlock block) {
            int positionCount = block.getPositionCount();
            for (int p = 0; p < positionCount; p++) {
                int first = block.getFirstValueIndex(p);
                int end = first + block.getValueCount(p);
                for (int valueIndex = first; valueIndex < end; valueIndex++) {
                    onDuplicateKeyPolicy.key(p, getAndSetOrdinal(block.getInt(valueIndex)));
                }
            }
        }

        private boolean getAndSetOrdinal(int ordinal) {
            if (ordinal < 0) {
                throw new IllegalArgumentException("ordinal key must be non-negative but was [" + ordinal + "]");
            }
            return seen.getAndSet(ordinal);
        }

        @Override
        public String toString() {
            return "Ordinal" + onDuplicateKeyPolicy.name + "Processor";
        }
    }
}
