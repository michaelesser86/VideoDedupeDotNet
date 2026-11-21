using System.Buffers.Binary;
using System.Numerics;

namespace VideoDedupe.Core;

/// <summary>
/// Robust audio fingerprint in the style of Haitsma &amp; Kalker: one 32-bit word per short,
/// overlapping frame, each bit the sign of an energy difference between neighbouring frequency
/// bands and consecutive frames. Re-encoding, resampling and volume changes flip few bits;
/// different audio differs in about half of them.
/// </summary>
public sealed class AudioFingerprint
{
    public const int SampleRate = 5512;
    private const int FrameSize = 2048;
    private const int Hop = 256;
    private const int Bands = 33;
    private const double MinHz = 300, MaxHz = 2000;
    // Frames this quiet carry no usable signal: silence would otherwise match silence.
    private const double SilenceRms = 80;

    /// <summary>Seconds of audio per fingerprint word.</summary>
    public const double SecondsPerWord = (double)Hop / SampleRate;

    public static AudioFingerprint Empty { get; } = new([], []);

    /// <summary>One word per frame, starting with the second frame (bits need a predecessor).</summary>
    public uint[] Words { get; }
    /// <summary>False where the frame or its predecessor is silent; such words are ignored.</summary>
    public bool[] Usable { get; }

    public bool IsEmpty => Words.Length == 0;

    public AudioFingerprint(uint[] words, bool[] usable)
    {
        if (words.Length != usable.Length)
            throw new ArgumentException("Every word needs a usable flag.", nameof(usable));
        Words = words;
        Usable = usable;
    }

    private static readonly int[] BandEdges = CreateBandEdges();
    private static readonly double[] Window = CreateWindow();

    /// <summary>Fingerprint of mono 16-bit samples at <see cref="SampleRate"/>.</summary>
    public static AudioFingerprint Compute(ReadOnlySpan<short> samples)
    {
        if (samples.Length < FrameSize + Hop)
            return Empty;

        var frames = (samples.Length - FrameSize) / Hop + 1;
        var re = new double[FrameSize];
        var im = new double[FrameSize];
        var previous = new double[Bands];
        var current = new double[Bands];
        var previousLoud = false;
        var words = new uint[frames - 1];
        var usable = new bool[frames - 1];

        for (var f = 0; f < frames; f++)
        {
            var frame = samples.Slice(f * Hop, FrameSize);
            double power = 0;
            for (var i = 0; i < FrameSize; i++)
            {
                power += (double)frame[i] * frame[i];
                re[i] = frame[i] * Window[i];
                im[i] = 0;
            }
            var loud = Math.Sqrt(power / FrameSize) >= SilenceRms;

            Fft(re, im);
            for (var b = 0; b < Bands; b++)
            {
                double energy = 0;
                for (var k = BandEdges[b]; k < BandEdges[b + 1]; k++)
                    energy += re[k] * re[k] + im[k] * im[k];
                current[b] = energy;
            }

            if (f > 0)
            {
                uint word = 0;
                for (var b = 0; b < Bands - 1; b++)
                    if (current[b] - current[b + 1] - (previous[b] - previous[b + 1]) > 0)
                        word |= 1u << b;
                words[f - 1] = word;
                usable[f - 1] = loud && previousLoud;
            }

            (previous, current) = (current, previous);
            previousLoud = loud;
        }
        return new AudioFingerprint(words, usable);
    }

    /// <summary>
    /// Lowest bit error rate over all alignments within ±<paramref name="maxOffsetWords"/>, or
    /// null when no alignment has <paramref name="minOverlapWords"/> usable words in both prints
    /// (silent, missing or too short audio). 0 means identical; unrelated audio is near 0.5.
    /// </summary>
    public static double? BitErrorRate(AudioFingerprint a, AudioFingerprint b, int maxOffsetWords,
        int minOverlapWords)
    {
        double? best = null;
        for (var offset = -maxOffsetWords; offset <= maxOffsetWords; offset++)
        {
            long errors = 0;
            var overlap = 0;
            var start = Math.Max(0, -offset);
            var end = Math.Min(a.Words.Length, b.Words.Length - offset);
            for (var i = start; i < end; i++)
            {
                var j = i + offset;
                if (!a.Usable[i] || !b.Usable[j]) continue;
                errors += BitOperations.PopCount(a.Words[i] ^ b.Words[j]);
                overlap++;
            }
            if (overlap < minOverlapWords) continue;
            var rate = errors / (32.0 * overlap);
            if (best is null || rate < best) best = rate;
        }
        return best;
    }

    /// <summary>Compact form for the database: count, words, then one byte per usable flag.</summary>
    public byte[] Serialize()
    {
        var data = new byte[4 + Words.Length * 5];
        BinaryPrimitives.WriteInt32LittleEndian(data, Words.Length);
        for (var i = 0; i < Words.Length; i++)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(data.AsSpan(4 + i * 4), Words[i]);
            data[4 + Words.Length * 4 + i] = Usable[i] ? (byte)1 : (byte)0;
        }
        return data;
    }

    /// <exception cref="InvalidDataException">The bytes were not produced by <see cref="Serialize"/>.</exception>
    public static AudioFingerprint Deserialize(ReadOnlySpan<byte> data)
    {
        if (data.Length < 4)
            throw new InvalidDataException("Audio fingerprint is truncated.");
        var count = BinaryPrimitives.ReadInt32LittleEndian(data);
        if (count < 0 || data.Length != 4 + (long)count * 5)
            throw new InvalidDataException("Audio fingerprint has an unexpected length.");
        var words = new uint[count];
        var usable = new bool[count];
        for (var i = 0; i < count; i++)
        {
            words[i] = BinaryPrimitives.ReadUInt32LittleEndian(data[(4 + i * 4)..]);
            usable[i] = data[4 + count * 4 + i] != 0;
        }
        return new AudioFingerprint(words, usable);
    }

    private static int[] CreateBandEdges()
    {
        var edges = new int[Bands + 1];
        for (var b = 0; b <= Bands; b++)
        {
            var hz = MinHz * Math.Pow(MaxHz / MinHz, (double)b / Bands);
            edges[b] = (int)Math.Round(hz * FrameSize / SampleRate);
        }
        return edges;
    }

    private static double[] CreateWindow()
    {
        var window = new double[FrameSize];
        for (var i = 0; i < FrameSize; i++)
            window[i] = 0.5 - 0.5 * Math.Cos(2 * Math.PI * i / (FrameSize - 1));
        return window;
    }

    // In-place iterative radix-2 FFT; the length is a power of two.
    private static void Fft(double[] re, double[] im)
    {
        var n = re.Length;
        for (int i = 1, j = 0; i < n; i++)
        {
            var bit = n >> 1;
            for (; (j & bit) != 0; bit >>= 1) j ^= bit;
            j ^= bit;
            if (i < j)
            {
                (re[i], re[j]) = (re[j], re[i]);
                (im[i], im[j]) = (im[j], im[i]);
            }
        }
        for (var length = 2; length <= n; length <<= 1)
        {
            var angle = -2 * Math.PI / length;
            double wRe = Math.Cos(angle), wIm = Math.Sin(angle);
            for (var i = 0; i < n; i += length)
            {
                double curRe = 1, curIm = 0;
                for (var k = 0; k < length / 2; k++)
                {
                    int a = i + k, b = i + k + length / 2;
                    var tRe = re[b] * curRe - im[b] * curIm;
                    var tIm = re[b] * curIm + im[b] * curRe;
                    re[b] = re[a] - tRe; im[b] = im[a] - tIm;
                    re[a] += tRe; im[a] += tIm;
                    (curRe, curIm) = (curRe * wRe - curIm * wIm, curRe * wIm + curIm * wRe);
                }
            }
        }
    }
}
