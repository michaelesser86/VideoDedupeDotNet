using VideoDedupe.Core;

namespace VideoDedupe.Tests;

public class DurationCandidatesTests
{
    [Theory]
    [InlineData(10.04, 10.06, 0.25, 1)]
    [InlineData(10, 11, 1, 1)]
    [InlineData(10, 11, 0.25, 0)]
    [InlineData(10, 10, 0, 1)]
    public void UsesActualToleranceAcrossResolutions(double a, double b, double tolerance, int count)
    {
        var files = new[] { (Duration: b, Width: 1920), (Duration: a, Width: 640) };
        var ranges = DurationCandidates.CreateRanges(files, f => f.Duration, tolerance, 2);
        Assert.Equal(count, ranges.Count);
        if (count > 0) Assert.Equal(2, ranges[0].Count);
    }

    [Fact]
    public void KeepsOverlappingPairsTogetherAndSplitsOnlyAtLargerGaps()
    {
        var ranges = DurationCandidates.CreateRanges(new double?[] { 11, 10, 10.5, 20, 20.5 }, d => d, 0.5, 2);
        Assert.Equal(2, ranges.Count);
        Assert.Equal(new double?[] { 10, 10.5, 11 }, ranges[0]);
        Assert.Equal(new double?[] { 20, 20.5 }, ranges[1]);
    }

    [Fact]
    public void IgnoresInvalidDurations()
    {
        var ranges = DurationCandidates.CreateRanges(new double?[] { null, double.NaN, double.PositiveInfinity, -1, 0, 10, 10 }, d => d, 0, 2);
        Assert.Equal(new double?[] { 10, 10 }, Assert.Single(ranges));
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(double.NaN)]
    [InlineData(double.PositiveInfinity)]
    public void RejectsInvalidTolerance(double tolerance)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            DurationCandidates.CreateRanges(new[] { 10.0 }, d => d, tolerance, 2));
    }
}
