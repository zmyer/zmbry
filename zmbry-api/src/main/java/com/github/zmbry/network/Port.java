package com.github.zmbry.network;

/**
 * @author zifeng
 *
 */
public class Port {
    private final int port;
    private final PortType type;

    public Port(int port, PortType type) {
        this.port = port;
        this.type = type;
    }

    public int getPort() {
        return this.port;
    }

    public PortType getPortType() {
        return this.type;
    }

    @Override
    public String toString() {
        return "Port[" + getPort() + ":" + getPortType() + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Port p = (Port) o;
        return p.port == port && p.type.equals(type);
    }
}
