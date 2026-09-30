#pragma once
#include <string>
#include <array>

#include "NationState.h"


namespace ModernWarCore
{
	std::string ValidateNation(const NationState& nation);
	std::string ValidateRoster(const std::array<NationState, 2>& nations);
}