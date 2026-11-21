using VideoDedupe.Core;
using VideoDedupe.Infrastructure;

namespace VideoDedupe.Tests;

public class MirrorAndAudioTests
{
    private static GrayFrame RandomFrame(int seed, int width = 96, int height = 54)
    {
        var random = new Random(seed);
        var pixels = new byte[width * height];
        // Smooth blobs instead of pixel noise, so the downscaled cells differ clearly.
        for (var y = 0; y < height; y++)
            for (var x = 0; x < width; x++)
                pixels[y * width + x] = (byte)(128 + 100 * Math.Sin(x * 0.11 + seed) * Math.Cos(y * 0.07 + seed * 0.5));
        pixels[random.Next(pixels.Length)] = 255;
        return new GrayFrame(width, height, pixels);
    }

    [Fact]
    public void MirrorHashEqualsHashOfMirroredFrame()
    {
        var hasher = new DHashService();
        var frame = RandomFrame(3);
        var hashes = hasher.ComputeHashes(frame);
        var mirroredHashes = hasher.ComputeHashes(frame.MirrorHorizontal());

        Assert.Equal(hashes.Mirrored, mirroredHashes.Normal);
        Assert.Equal(hashes.Normal, mirroredHashes.Mirrored);
        Assert.True(DHashService.HammingDistance(hashes.Normal, mirroredHashes.Normal) > 10,
            "an asymmetric picture must not match its mirror image directly");
    }

    [Fact]
    public void CompareMatchesMirroredCopyOnlyWhenEnabled()
    {
        var hasher = new DHashService();
        var frames = new[] { 20, 50, 80 }.ToDictionary(p => p, p => RandomFrame(p));
        var original = frames.ToDictionary(f => f.Key, f => hasher.ComputeHashes(f.Value));
        var mirrored = frames.ToDictionary(f => f.Key, f => hasher.ComputeHashes(f.Value.MirrorHorizontal()));
        var file = new AppDb.MediaFileRow { Id = 2 };

        var match = DupeBuildService.Compare(original, mirrored, file, matchMirrored: true);
        Assert.True(match.Mirrored);
        Assert.Equal(0, match.Avg);

        var plain = DupeBuildService.Compare(original, mirrored, file, matchMirrored: false);
        Assert.False(plain.Mirrored);
        Assert.True(plain.Avg > 10);
    }

    // A stepped melody: a different tone sequence for every seed.
    private static short[] Melody(int seed, double seconds, double volume = 0.5, double noise = 0)
    {
        var random = new Random(seed);
        // Part of the recording: real soundtracks fill every band, not just a few tones.
        var backgroundRandom = new Random(seed + 500);
        // Added by a lossy copy: independent of the recording.
        var noiseRandom = new Random(seed + 1000);
        var samples = new short[(int)(seconds * AudioFingerprint.SampleRate)];
        var notes = Enumerable.Range(0, (int)(seconds * 4) + 1).Select(_ => 200 + random.Next(0, 16) * 70.0).ToArray();
        double background = 0;
        for (var i = 0; i < samples.Length; i++)
        {
            var t = (double)i / AudioFingerprint.SampleRate;
            var f = notes[(int)(t * 4)];
            background = 0.9 * background + 0.1 * (backgroundRandom.NextDouble() * 2 - 1);
            var envelope = 0.5 + 0.5 * Math.Sin(2 * Math.PI * 0.7 * t);
            var value = Math.Sin(2 * Math.PI * f * t) * 0.5 + Math.Sin(2 * Math.PI * f * 1.5 * t) * 0.25
                        + background * 2.0 * envelope
                        + noise * (noiseRandom.NextDouble() * 2 - 1);
            samples[i] = (short)Math.Clamp(value * volume * short.MaxValue, short.MinValue, short.MaxValue);
        }
        return samples;
    }

    private static double? Ber(short[] a, short[] b) => AudioFingerprint.BitErrorRate(
        AudioFingerprint.Compute(a), AudioFingerprint.Compute(b), maxOffsetWords: 100, minOverlapWords: 200);

    [Fact]
    public void SameAudioMatchesDespiteVolumeNoiseAndOffset()
    {
        var original = Melody(1, 40);
        var quieterNoisy = Melody(1, 40, volume: 0.25, noise: 0.05);
        // The same audio starting 1.5 s later, e.g. a slightly differently cut copy. 1.5 s is not a
        // whole number of fingerprint frames, so the frames do not line up exactly, as with real copies.
        var shifted = original.Skip((int)(1.5 * AudioFingerprint.SampleRate)).ToArray();

        Assert.Equal(0, Ber(original, original));
        Assert.True(Ber(original, quieterNoisy) < 0.15, $"noisy ber={Ber(original, quieterNoisy)}");
        Assert.True(Ber(original, shifted) < 0.15, $"shifted ber={Ber(original, shifted)}");
    }

    [Fact]
    public void DifferentAudioDoesNotMatch()
    {
        for (var seed = 2; seed < 6; seed++)
        {
            var ber = Ber(Melody(1, 40), Melody(seed, 40));
            Assert.True(ber > 0.35, $"seed {seed}: ber={ber}");
        }
    }

    [Fact]
    public void SilenceAndShortAudioAreNotComparable()
    {
        var silence = new short[40 * AudioFingerprint.SampleRate];
        Assert.Null(Ber(silence, silence));
        Assert.Null(Ber(Melody(1, 40), silence));
        Assert.True(AudioFingerprint.Compute(new short[100]).IsEmpty);
    }

    [Fact]
    public void FingerprintSurvivesSerialization()
    {
        var print = AudioFingerprint.Compute(Melody(7, 10));
        var copy = AudioFingerprint.Deserialize(print.Serialize());

        Assert.Equal(print.Words, copy.Words);
        Assert.Equal(print.Usable, copy.Usable);
        Assert.True(AudioFingerprint.Deserialize(AudioFingerprint.Empty.Serialize()).IsEmpty);
        Assert.Throws<InvalidDataException>(() => AudioFingerprint.Deserialize(new byte[] { 5, 0, 0, 0, 1 }));
    }
}
