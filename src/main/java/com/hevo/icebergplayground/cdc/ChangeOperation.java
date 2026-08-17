package com.hevo.icebergplayground.cdc;

/** The kind of row-level change a {@link ChangeRecord} represents. */
public enum ChangeOperation {
    INSERT,
    UPDATE,
    DELETE
}
