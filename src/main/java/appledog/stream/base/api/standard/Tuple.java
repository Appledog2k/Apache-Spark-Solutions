package appledog.stream.base.api.standard;

import java.io.Serializable;

public class Tuple<A,B> implements Serializable {
    final transient A key;
    final transient B value;

    public Tuple(A key, B value) {
        this.key = key;
        this.value = value;
    }

    public A getKey() {
        return key;
    }

    public B getValue() {
        return value;
    }

    @Override
    public boolean equals(final Object other) {
        if (other == null) {
            return false;
        }
        if (other == this) {
            return true;
        }
        if (!(other instanceof Tuple)) {
            return false;
        }

        final Tuple<?, ?> tuple = (Tuple<?, ?>) other;
        if (key == null) {
            if (tuple.key != null) {
                return false;
            }
        } else {
            if (!key.equals(tuple.key)) {
                return false;
            }
        }

        if (value == null) {
            if (tuple.value != null) {
                return false;
            }
        } else {
            if (!value.equals(tuple.value)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        return 581 + (this.key == null ? 0 : this.key.hashCode()) + (this.value == null ? 0 : this.value.hashCode());
    }
}
