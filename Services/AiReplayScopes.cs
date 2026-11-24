using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Coflnet.Sky.Sniper.Services;

#nullable enable

/// <summary>
/// The replay scopes (<see cref="IAiFinder.BeginReplay"/>) of every tag the AI finder values, for a host that replays stored
/// sales of all tags at once. A tag's scope can be ended on its own as soon as its replay is done; disposing ends the rest.
/// </summary>
public sealed class AiReplayScopes : IDisposable
{
    private readonly ConcurrentDictionary<string, IDisposable> open = new(StringComparer.OrdinalIgnoreCase);

    /// <remarks>When opening one scope fails, the ones opened before are ended again</remarks>
    public AiReplayScopes(IAiFinder finder)
    {
        Tags = finder.ActiveTags;
        try
        {
            foreach (var tag in Tags)
                open.TryAdd(tag, finder.BeginReplay(tag));
        }
        catch
        {
            Dispose();
            throw;
        }
    }

    /// <summary>The tags whose scopes were opened, in the finder's order</summary>
    public IReadOnlyList<string> Tags { get; }

    /// <summary>Ends the scope of one tag; ending it again does nothing</summary>
    public void End(string tag)
    {
        if (open.TryRemove(tag, out var scope))
            scope.Dispose();
    }

    public void Dispose()
    {
        foreach (var tag in open.Keys)
            End(tag);
    }
}
