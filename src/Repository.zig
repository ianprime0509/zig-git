const std = @import("std");
const mem = std.mem;
const git = @import("git.zig");
const Oid = git.Oid;
const Odb = @import("Odb.zig");
const pack = @import("pack.zig");
const protocol = @import("protocol.zig");

odb: Odb,

const Repository = @This();

pub const CloneOptions = struct {
    /// The commit depth to clone. 0 means clone all history.
    depth: u32 = 0,
    /// The ref to clone. null means the default branch.
    ref: ?[]const u8 = null,
};

/// Clones a repository.
///
/// Note: this function is very rudimentary. It only supports single-branch
/// clones over HTTP(S).
pub fn clone(allocator: mem.Allocator, uri: std.Uri, dir: std.fs.Dir, options: CloneOptions) !void {
    var transport = std.http.Client{ .allocator = allocator };
    defer transport.deinit();
    var client = protocol.Client{ .transport = transport };

    var supports_agent = false;
    var supports_shallow = false;
    {
        var capability_iterator = try client.getCapabilities(allocator, uri);
        defer capability_iterator.deinit();
        while (try capability_iterator.next()) |capability| {
            if (mem.eql(u8, capability.key, "agent")) {
                supports_agent = true;
            } else if (mem.eql(u8, capability.key, "fetch")) {
                var feature_iterator = mem.splitScalar(u8, capability.value orelse continue, ' ');
                while (feature_iterator.next()) |feature| {
                    if (mem.eql(u8, feature, "shallow")) {
                        supports_shallow = true;
                    }
                }
            }
        }
    }

    const want_oid = want_oid: {
        if (options.ref) |ref| {
            if (git.isOid(ref)) break :want_oid ref;
        }
        @panic("TODO: discover matching refs from remote");
    };

    var pack_dir = pack_dir: {
        const pack_dir_path = try std.fs.path.join(allocator, &.{ ".git", "objects", "pack" });
        defer allocator.free(pack_dir_path);
        break :pack_dir try dir.makeOpenPath(pack_dir_path, .{});
    };
    defer pack_dir.close();
    const pack_file_stem = pack_file_stem: {
        const pack_hash = pack_hash: {
            var pack_file = try pack_dir.createFile("tmp.pack", .{ .read = true });
            defer pack_file.close();
            var fetch_stream = try client.fetch(allocator, uri, &.{want_oid}, .{
                .agent = if (supports_agent) protocol.Client.standard_agent else null,
            });
            defer fetch_stream.deinit();
            var fifo = std.fifo.LinearFifo(u8, .{ .Static = 4096 }).init();
            try fifo.pump(fetch_stream.reader(), pack_file.writer());
            try pack_file.sync();
            try pack_file.seekFromEnd(-git.object_name_length);
            const pack_hash = try pack_file.reader().readBytesNoEof(git.object_name_length);
            break :pack_hash try std.fmt.allocPrint(allocator, "{}", .{std.fmt.fmtSliceHexLower(&pack_hash)});
        };
        defer allocator.free(pack_hash);

        break :pack_file_stem try std.fmt.allocPrint(allocator, "pack-{s}", .{pack_hash});
    };
    defer allocator.free(pack_file_stem);
    const pack_file_name = try std.fmt.allocPrint(allocator, "{s}.pack", .{pack_file_stem});
    defer allocator.free(pack_file_name);
    try pack_dir.rename("tmp.pack", pack_file_name);
    var pack_file = try pack_dir.openFile(pack_file_name, .{});
    defer pack_file.close();

    const index_file_name = try std.fmt.allocPrint(allocator, "{s}.idx", .{pack_file_stem});
    defer allocator.free(index_file_name);
    var index_file = try pack_dir.createFile(index_file_name, .{ .read = true });
    defer index_file.close();
    var index_buffered_writer = std.io.bufferedWriter(index_file.writer());
    try pack.indexPack(allocator, pack_file, index_buffered_writer.writer());
    try index_buffered_writer.flush();
    try index_file.sync();

    try pack_file.seekTo(0);
    try index_file.seekTo(0);
    var odb = try Odb.init(pack_file, index_file);
    var repository = Repository{ .odb = odb };
    try repository.checkout(allocator, dir, try git.parseOid(want_oid));
}

pub fn checkout(repository: *Repository, allocator: mem.Allocator, worktree: std.fs.Dir, commit_oid: Oid) !void {
    try repository.odb.seekOid(commit_oid);
    const tree_oid = tree_oid: {
        var commit_object = try repository.odb.readObject(allocator);
        defer commit_object.deinit(allocator);
        if (commit_object.type != .commit) return error.NotACommit;
        break :tree_oid try getCommitTree(commit_object.data);
    };
    try repository.checkoutTree(allocator, worktree, tree_oid);
}

fn checkoutTree(repository: *Repository, allocator: mem.Allocator, dir: std.fs.Dir, tree_oid: Oid) !void {
    try repository.odb.seekOid(tree_oid);
    var tree_object = try repository.odb.readObject(allocator);
    defer tree_object.deinit(allocator);
    if (tree_object.type != .tree) return error.NotATree;

    var tree_iter = TreeIterator.init(tree_object.data);
    while (try tree_iter.next()) |entry| {
        switch (entry.type) {
            .directory => {
                try dir.makeDir(entry.name);
                var subdir = try dir.openDir(entry.name, .{});
                defer subdir.close();
                try repository.checkoutTree(allocator, subdir, entry.oid);
            },
            .file => {
                var file = try dir.createFile(entry.name, .{});
                defer file.close();
                try repository.odb.seekOid(entry.oid);
                var file_object = try repository.odb.readObject(allocator);
                defer file_object.deinit(allocator);
                if (file_object.type != .blob) return error.InvalidFile;
                try file.writeAll(file_object.data);
                try file.sync();
            },
            .symlink => return error.SymlinkNotSupported,
            .gitlink => {
                // Consistent with git archive behavior, create the directory but
                // do nothing else
                try dir.makeDir(entry.name);
            },
        }
    }
}

fn getCommitTree(commit_data: []const u8) !Oid {
    if (!mem.startsWith(u8, commit_data, "tree ") or
        commit_data.len < "tree ".len + git.fmt_object_name_length + "\n".len or
        commit_data["tree ".len + git.fmt_object_name_length] != '\n')
    {
        return error.InvalidCommit;
    }
    return try git.parseOid(commit_data["tree ".len..][0..git.fmt_object_name_length]);
}

pub const TreeIterator = struct {
    data: []const u8,
    pos: usize = 0,

    pub fn init(data: []const u8) TreeIterator {
        return .{ .data = data };
    }

    pub const Entry = struct {
        type: Type,
        executable: bool,
        name: [:0]const u8,
        oid: Oid,

        pub const Type = enum(u4) {
            directory = 0o4,
            file = 0o10,
            symlink = 0o12,
            gitlink = 0o16,
        };
    };

    pub fn next(iterator: *TreeIterator) !?Entry {
        if (iterator.pos == iterator.data.len) return null;

        const mode_end = mem.indexOfScalarPos(u8, iterator.data, iterator.pos, ' ') orelse return error.InvalidTree;
        const mode: packed struct {
            permission: u9,
            unused: u3,
            type: u4,
        } = @bitCast(std.fmt.parseUnsigned(u16, iterator.data[iterator.pos..mode_end], 8) catch return error.InvalidTree);
        const @"type" = std.meta.intToEnum(Entry.Type, mode.type) catch return error.InvalidTree;
        const executable = switch (mode.permission) {
            0 => if (@"type" == .file) return error.InvalidTree else false,
            0o644 => if (@"type" != .file) return error.InvalidTree else false,
            0o755 => if (@"type" != .file) return error.InvalidTree else true,
            else => return error.InvalidTree,
        };
        iterator.pos = mode_end + 1;

        const name_end = mem.indexOfScalarPos(u8, iterator.data, iterator.pos, 0) orelse return error.InvalidTree;
        const name = iterator.data[iterator.pos..name_end :0];
        iterator.pos = name_end + 1;

        if (iterator.pos + git.object_name_length > iterator.data.len) return error.InvalidTree;
        const oid = iterator.data[iterator.pos..][0..git.object_name_length].*;
        iterator.pos += git.object_name_length;

        return .{ .type = @"type", .executable = executable, .name = name, .oid = oid };
    }
};
