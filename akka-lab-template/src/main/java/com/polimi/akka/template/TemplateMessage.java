package com.polimi.akka.template;

import java.io.Serializable;

/**
 * EXAM TEMPLATE: Immutable and Serializable Message
 * <p>
 * Rules for Akka messages:
 * 1. All fields MUST be {@code private final} (immutable).
 * 2. Collections or arrays passed in should be defensively copied or wrapped with {@code List.copyOf(...)}.
 * 3. Implement {@link Serializable} for remote/cluster compatibility.
 * </p>
 */
public class TemplateMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String content;
    private final int code;

    public TemplateMessage(String content) {
        this(content, 0);
    }

    public TemplateMessage(String content, int code) {
        this.content = content;
        this.code = code;
    }

    public String getContent() {
        return this.content;
    }

    public int getCode() {
        return this.code;
    }

    @Override
    public String toString() {
        return "TemplateMessage{content='" + content + "', code=" + code + "}";
    }
}

