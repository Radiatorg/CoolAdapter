package com.foranx.cooladapter.config;

import java.util.Arrays;

public record Credentials(String username, char[] password) {

    @Override
    public String toString() {
        return username + "/******";
    }

    public void clear() {
        Arrays.fill(password, '\0');
    }
}