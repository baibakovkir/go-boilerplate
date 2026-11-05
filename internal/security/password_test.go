package security

import "testing"

func TestPasswordHash(t *testing.T) {
	hash, e := HashPassword("пароль6")
	if e != nil {
		t.Fatal(e)
	}
	if !CheckPassword(hash, "пароль6") {
		t.Fatal("matching password rejected")
	}
	if CheckPassword(hash, "пароль7") {
		t.Fatal("wrong password accepted")
	}
	for _, bad := range []string{
		"",
		"$argon2id$v=19$m=4294967295,t=2,p=1$eA$eA",
		"$argon2id$v=19$m=19456,t=0,p=1$eA$eA",
	} {
		if CheckPassword(bad, "secret") {
			t.Fatal("malformed hash accepted")
		}
	}
	second, _ := HashPassword("пароль6")
	if hash == second {
		t.Fatal("salt was reused")
	}
}

func BenchmarkPasswordHash(b *testing.B) {
	for b.Loop() {
		if _, e := HashPassword("secret123"); e != nil {
			b.Fatal(e)
		}
	}
}
