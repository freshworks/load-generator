// From https://golang.org/doc/codewalk/markov/

// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lua

import (
	"bufio"
	"fmt"
	"math/rand"
	"strings"
)

// MarkovPrefix is a Markov chain prefix of one or more words.
type MarkovPrefix []string

// String returns the MarkovPrefix as a string (for use as a map key).
func (p MarkovPrefix) String() string {
	return strings.Join(p, " ")
}

// Shift removes the first word from the MarkovPrefix and appends the given word.
func (p MarkovPrefix) Shift(word string) {
	copy(p, p[1:])
	p[len(p)-1] = word
}

// MarkovChain contains a map ("chain") of prefixes to a list of suffixes.
// A prefix is a string of prefixLen words joined with spaces.
// A suffix is a single word. A prefix can have multiple suffixes.
type MarkovChain struct {
	chain     map[string][]string
	prefixLen int
}

// NewMarkovChain returns a new MarkovChain with prefixes of prefixLen words.
func NewMarkovChain(prefixLen int) *MarkovChain {
	return &MarkovChain{make(map[string][]string), prefixLen}
}

// Build reads text from the provided Reader and
// parses it into prefixes and suffixes that are stored in MarkovChain.
func (c *MarkovChain) Build() {
	br := bufio.NewReader(strings.NewReader(corpus))
	p := make(MarkovPrefix, c.prefixLen)
	for {
		var s string
		if _, err := fmt.Fscan(br, &s); err != nil {
			break
		}
		key := p.String()
		c.chain[key] = append(c.chain[key], s)
		p.Shift(s)
	}
}

// Generate returns a string of at most n words generated from MarkovChain.
func (c *MarkovChain) Generate(n int) string {
	p := make(MarkovPrefix, c.prefixLen)
	var words strings.Builder
	for i := 0; i < n; i++ {
		choices := c.chain[p.String()]
		if len(choices) == 0 {
			break
		}
		next := choices[rand.Intn(len(choices))]
		fmt.Fprintf(&words, "%s ", next)
		p.Shift(next)
	}
	return words.String()
}

// GenerateText returns a string of >= given size in bytes, with a para for
// even N words specified
func (c *MarkovChain) GenerateText(size int, paraNumWords int) string {
	var text strings.Builder

	if paraNumWords == 0 {
		paraNumWords = 1
	}

	for text.Len() < size {
		p := make(MarkovPrefix, c.prefixLen)
		for i := 0; i <= paraNumWords; i++ {
			choices := c.chain[p.String()]
			if len(choices) == 0 || text.Len() >= size {
				break
			}
			next := choices[rand.Intn(len(choices))]
			fmt.Fprintf(&text, "%s ", next)
			p.Shift(next)
		}
		fmt.Fprintf(&text, "\n\n")
	}

	return text.String()
}

// TODO: Corpus is hard coded for now, optionally should take it from
// cmdline params
//
// Corpus from http://www.gutenberg.org/cache/epub/4363/pg4363.txt
var corpus string = `The Will to Truth, which is to tempt us to many a hazardous
enterprise, the famous Truthfulness of which all philosophers have
hitherto spoken with respect, what questions has this Will to Truth not
laid before us! What strange, perplexing, questionable questions! It is
already a long story; yet it seems as if it were hardly commenced. Is
it any wonder if we at last grow distrustful, lose patience, and turn
impatiently away? That this Sphinx teaches us at last to ask questions
ourselves? WHO is it really that puts questions to us here? WHAT really
is this "Will to Truth" in us? In fact we made a long halt at the
question as to the origin of this Will--until at last we came to an
absolute standstill before a yet more fundamental question. We inquired
about the VALUE of this Will. Granted that we want the truth: WHY NOT
RATHER untruth? And uncertainty? Even ignorance? The problem of the
value of truth presented itself before us--or was it we who presented
ourselves before the problem? Which of us is the Oedipus here? Which
the Sphinx? It would seem to be a rendezvous of questions and notes of
interrogation. And could it be believed that it at last seems to us as
if the problem had never been propounded before, as if we were the first
to discern it, get a sight of it, and RISK RAISING it? For there is risk
in raising it, perhaps there is no greater risk.

"HOW COULD anything originate out of its opposite? For example, truth
out of error? or the Will to Truth out of the will to deception? or the
generous deed out of selfishness? or the pure sun-bright vision of the
wise man out of covetousness? Such genesis is impossible; whoever dreams
of it is a fool, nay, worse than a fool; things of the highest
value must have a different origin, an origin of THEIR own--in this
transitory, seductive, illusory, paltry world, in this turmoil of
delusion and cupidity, they cannot have their source. But rather in
the lap of Being, in the intransitory, in the concealed God, in the
Thing-in-itself--THERE must be their source, and nowhere else!--This
mode of reasoning discloses the typical prejudice by which
metaphysicians of all times can be recognized, this mode of valuation
is at the back of all their logical procedure; through this "belief" of
theirs, they exert themselves for their "knowledge," for something that
is in the end solemnly christened "the Truth." The fundamental belief of
metaphysicians is THE BELIEF IN ANTITHESES OF VALUES. It never occurred
even to the wariest of them to doubt here on the very threshold (where
doubt, however, was most necessary); though they had made a solemn
vow, "DE OMNIBUS DUBITANDUM." For it may be doubted, firstly, whether
antitheses exist at all; and secondly, whether the popular valuations
and antitheses of value upon which metaphysicians have set their
seal, are not perhaps merely superficial estimates, merely provisional
perspectives, besides being probably made from some corner, perhaps from
below--"frog perspectives," as it were, to borrow an expression current
among painters. In spite of all the value which may belong to the true,
the positive, and the unselfish, it might be possible that a higher
and more fundamental value for life generally should be assigned to
pretence, to the will to delusion, to selfishness, and cupidity. It
might even be possible that WHAT constitutes the value of those good and
respected things, consists precisely in their being insidiously
related, knotted, and crocheted to these evil and apparently opposed
things--perhaps even in being essentially identical with them. Perhaps!
But who wishes to concern himself with such dangerous "Perhapses"!
For that investigation one must await the advent of a new order of
philosophers, such as will have other tastes and inclinations, the
reverse of those hitherto prevalent--philosophers of the dangerous
"Perhaps" in every sense of the term. And to speak in all seriousness, I
see such new philosophers beginning to appear.

Having kept a sharp eye on philosophers, and having read between
their lines long enough, I now say to myself that the greater part of
conscious thinking must be counted among the instinctive functions, and
it is so even in the case of philosophical thinking; one has here to
learn anew, as one learned anew about heredity and "innateness." As
little as the act of birth comes into consideration in the whole process
and procedure of heredity, just as little is "being-conscious" OPPOSED
to the instinctive in any decisive sense; the greater part of the
conscious thinking of a philosopher is secretly influenced by his
instincts, and forced into definite channels. And behind all logic and
its seeming sovereignty of movement, there are valuations, or to speak
more plainly, physiological demands, for the maintenance of a definite
mode of life For example, that the certain is worth more than the
uncertain, that illusion is less valuable than "truth" such valuations,
in spite of their regulative importance for US, might notwithstanding be
only superficial valuations, special kinds of _niaiserie_, such as may
be necessary for the maintenance of beings such as ourselves. Supposing,
in effect, that man is not just the "measure of things."

It seems to me that there is everywhere an attempt at present to
divert attention from the actual influence which Kant exercised on
German philosophy, and especially to ignore prudently the value which
he set upon himself. Kant was first and foremost proud of his Table of
Categories; with it in his hand he said: "This is the most difficult
thing that could ever be undertaken on behalf of metaphysics." Let us
only understand this "could be"! He was proud of having DISCOVERED a
new faculty in man, the faculty of synthetic judgment a priori. Granting
that he deceived himself in this matter; the development and rapid
flourishing of German philosophy depended nevertheless on his pride, and
on the eager rivalry of the younger generation to discover if possible
something--at all events "new faculties"--of which to be still
prouder!--But let us reflect for a moment--it is high time to do so.
"How are synthetic judgments a priori POSSIBLE?" Kant asks himself--and
what is really his answer? "BY MEANS OF A MEANS (faculty)"--but
unfortunately not in five words, but so circumstantially, imposingly,
and with such display of German profundity and verbal flourishes, that
one altogether loses sight of the comical niaiserie allemande involved
in such an answer. People were beside themselves with delight over this
new faculty, and the jubilation reached its climax when Kant further
discovered a moral faculty in man--for at that time Germans were still
moral, not yet dabbling in the "Politics of hard fact." Then came
the honeymoon of German philosophy. All the young theologians of the
Tubingen institution went immediately into the groves--all seeking for
"faculties." And what did they not find--in that innocent, rich, and
still youthful period of the German spirit, to which Romanticism, the
malicious fairy, piped and sang, when one could not yet distinguish
between "finding" and "inventing"! Above all a faculty for the
"transcendental"; Schelling christened it, intellectual intuition,
and thereby gratified the most earnest longings of the naturally
pious-inclined Germans. One can do no greater wrong to the whole of
this exuberant and eccentric movement (which was really youthfulness,
notwithstanding that it disguised itself so boldly, in hoary and senile
conceptions), than to take it seriously, or even treat it with moral
indignation. Enough, however--the world grew older, and the dream
vanished. A time came when people rubbed their foreheads, and they still
rub them today. People had been dreaming, and first and foremost--old
Kant. "By means of a means (faculty)"--he had said, or at least meant to
say. But, is that--an answer? An explanation? Or is it not rather merely
a repetition of the question? How does opium induce sleep? "By means of
a means (faculty)," namely the virtus dormitiva, replies the doctor in
Moliere,

    Quia est in eo virtus dormitiva,
    Cujus est natura sensus assoupire.

But such replies belong to the realm of comedy, and it is high time
to replace the Kantian question, "How are synthetic judgments a PRIORI
possible?" by another question, "Why is belief in such judgments
necessary?"--in effect, it is high time that we should understand
that such judgments must be believed to be true, for the sake of the
preservation of creatures like ourselves; though they still might
naturally be false judgments! Or, more plainly spoken, and roughly and
readily--synthetic judgments a priori should not "be possible" at all;
we have no right to them; in our mouths they are nothing but false
judgments. Only, of course, the belief in their truth is necessary, as
plausible belief and ocular evidence belonging to the perspective view
of life. And finally, to call to mind the enormous influence which
"German philosophy"--I hope you understand its right to inverted commas
(goosefeet)?--has exercised throughout the whole of Europe, there is
no doubt that a certain VIRTUS DORMITIVA had a share in it; thanks to
German philosophy, it was a delight to the noble idlers, the virtuous,
the mystics, the artiste, the three-fourths Christians, and the
political obscurantists of all nations, to find an antidote to the still
overwhelming sensualism which overflowed from the last century into
this, in short--"sensus assoupire."...`
