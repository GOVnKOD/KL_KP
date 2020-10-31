#encoding "utf-8"

PersonName -> Word<kwtype="имя">;
Places->Noun<kwtype = places>;
PlaceO->AnyWord* interp(Fact.Person = " ") Places interp(Fact.Place) AnyWord* | Places interp(Fact.Place) AnyWord interp(Fact.Person = " ") AnyWord*;
PlasecO->AnyWord* PersonName interp(Fact.Person) AnyWord* Places interp(Fact.Place) AnyWord* | AnyWord* Places interp(Fact.Place) AnyWord* PersonName interp(Fact.Person) AnyWord*;
PersonO->AnyWord* interp(Fact.Place = " ") PersonName interp(Fact.Person) AnyWord*| PersonName interp(Fact.Person) AnyWord interp(Fact.Place = " ") AnyWord*;
S ->PlasecO|PersonO|PlaceO ;