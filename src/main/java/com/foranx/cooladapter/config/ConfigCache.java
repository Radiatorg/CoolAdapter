package com.foranx.cooladapter.config;

import java.nio.file.attribute.FileTime;

public record ConfigCache(FolderConfig config, FileTime lastModified) {}