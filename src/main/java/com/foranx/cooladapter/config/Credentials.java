package com.foranx.cooladapter.config;

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

public record Credentials(String username, char[] password) {

    public Credentials {
        password = password.clone();
    }

    @Override
    public @NotNull String toString() {
        return username + "/******";
    }

    public void clear() {
        Arrays.fill(password, '\0');
    }
}
